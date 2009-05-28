/* ----------------------------------------------------------------------
   MR-MPI = MapReduce-MPI library
   http://www.cs.sandia.gov/~sjplimp/mapreduce.html
   Steve Plimpton, sjplimp@sandia.gov, Sandia National Laboratories

   Copyright (2009) Sandia Corporation.  Under the terms of Contract
   DE-AC04-94AL85000 with Sandia Corporation, the U.S. Government retains
   certain rights in this software.  This software is distributed under 
   the modified Berkeley Software Distribution (BSD) License.

   See the README file in the top-level MapReduce directory.
------------------------------------------------------------------------- */

#include "mpi.h"
#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "stdint.h"
#include "keymultivalue.h"
#include "keyvalue.h"
#include "spool.h"
#include "hash.h"
#include "memory.h"
#include "error.h"

using namespace MAPREDUCE_NS;

#define MIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

#define ROUNDUP(A,B) (char *) (((unsigned long) A + B) & ~B);

#define ALIGNFILE 512              // same as in mapreduce.cpp
#define PARTITIONCHUNK 1
#define SETCHUNK 1
#define PAGECHUNK 16
#define SPOOLMBYTES 1

/* ---------------------------------------------------------------------- */

KeyMultiValue::KeyMultiValue(MPI_Comm comm_caller,
			     char *memblock, int memsize,
			     int memkalign, int memvalign, int counter)
{
  comm = comm_caller;
  MPI_Comm_rank(comm,&me);

  memory = new Memory(comm);
  error = new Error(comm);

  sprintf(filename,"mrmpi.kmv.%d.%d",counter,me);
  fileflag = 0;
  fp = NULL;

  pages = NULL;
  npage = maxpage = 0;

  page = memblock;
  pagesize = memsize;

  // talign = max of (kalign,valign,int)

  kalign = memkalign;
  valign = memvalign;
  talign = MAX(kalign,valign);
  talign = MAX(talign,sizeof(int));

  kalignm1 = kalign-1;
  valignm1 = valign-1;
  talignm1 = talign-1;

  twolenbytes = 2*sizeof(int);
  threelenbytes = 3*sizeof(int);

  nkmv = ksize = vsize = tsize = 0;
  init_page();
}

/* ---------------------------------------------------------------------- */

KeyMultiValue::~KeyMultiValue()
{
  delete memory;
  delete error;

  memory->sfree(pages);
  if (fileflag) remove(filename);
}

/* ----------------------------------------------------------------------
   copy contents of another KMV into me
   input KMV should never be self
------------------------------------------------------------------------- */

void KeyMultiValue::copy(KeyMultiValue *kmv)
{
  if (kmv == this) error->all("Cannot perform KeyMultiValue copy on self");

  char *page_hold = page;
  int npage_other = kmv->request_info(&page);

  for (int ipage = 0; ipage < npage_other-1; ipage++) {
    nkey = kmv->request_page(ipage,0);
    create_page();
    write_page();
    npage++;
  }

  nkey = kmv->request_page(npage_other-1,0);
  complete();
  page = page_hold;
}

/* ----------------------------------------------------------------------
   complete the KMV after data has been added to it
   called by MR methods after creating & populating a KMV
------------------------------------------------------------------------- */

void KeyMultiValue::complete()
{
  create_page();

  // if disk file exists, write last page, close file

  if (fileflag) {
    write_page();
    fclose(fp);
    fp = NULL;
  }

  npage++;
  init_page();

  // set sizes for entire KMV

  nkmv = ksize = vsize = tsize = 0;
  for (int ipage = 0; ipage < npage; ipage++) {
    nkmv += pages[ipage].nkey;
    ksize += pages[ipage].keysize;
    vsize += pages[ipage].valuesize;
    tsize += pages[ipage].exactsize;
  }
}

/* ----------------------------------------------------------------------
   return # of pages and ptr to in-memory page
------------------------------------------------------------------------- */

int KeyMultiValue::request_info(char **ptr)
{
  *ptr = page;
  return npage;
}

/* ----------------------------------------------------------------------
   ready a page of KMV data
   caller is looping over data in KMV
   writeflag = 0 when called by MR::compress() or MR::reduce() or copy()
   writeflag = 1 when called by MR::sort_multivalues()
------------------------------------------------------------------------- */

int KeyMultiValue::request_page(int ipage, int writeflag)
{
  // load page from file if necessary

  if (fileflag) read_page(ipage,writeflag);

  // close file if last page

  if (ipage == npage-1 && fileflag) {
    fclose(fp);
    fp = NULL;
  }

  return pages[ipage].nkey;
}

/* ----------------------------------------------------------------------
   write out a changed page of KMV data
   called by MR::sort_multivalues()
------------------------------------------------------------------------- */

void KeyMultiValue::overwrite_page(int ipage)
{
  if (!fileflag) return;
  write_page();

  // close file if last page

  if (ipage == npage-1) {
    fclose(fp);
    fp = NULL;
  }
}

/* ----------------------------------------------------------------------
   add a key/value pair as a one-value KMV
   called by clone()
------------------------------------------------------------------------- */

void KeyMultiValue::add(char *key, int keybytes, char *value, int valuebytes)
{
  char *iptr = &page[alignsize];
  char *kptr = iptr + 5*sizeof(int);
  kptr = ROUNDUP(kptr,kalignm1);
  char *vptr = kptr + keybytes;
  vptr = ROUNDUP(vptr,valignm1);
  char *nptr = vptr + valuebytes;
  nptr = ROUNDUP(nptr,talignm1);
  int kmvbytes = nptr - iptr;
  
  // page is full, write to disk
  
  if (alignsize + kmvbytes > pagesize) {
    if (alignsize == 0)
      error->one("Single KMV pair exceeds KeyMultiValue page size");
    
    create_page();
    write_page();
    npage++;
    init_page();
    add(key,keybytes,value,valuebytes);
    return;
  }

  int *intptr = (int *) iptr;
  *(intptr++) = keybytes;
  *(intptr++) = valuebytes;
  *(intptr++) = 1;
  *(intptr++) = valuebytes;
  memcpy(kptr,key,keybytes);
  memcpy(vptr,value,valuebytes);

  nkey++;
  nvalue++;
  keysize += keybytes;
  valuesize += valuebytes;
  alignsize += kmvbytes;
}

/* ----------------------------------------------------------------------
   clone a KV directly into a KMV, one KV pair -> one KMV pair
   each processor works on only its data
   called by MR::clone()
------------------------------------------------------------------------- */

void KeyMultiValue::clone(KeyValue *kv)
{
  // loop over KV, turning each KV pair into a KMV pair

  int kdummy,vdummy,adummy;
  int nkey_kv,keybytes,valuebytes;
  char *ptr,*key,*value;

  char *page_kv;
  int npage_kv = kv->request_info(&page_kv);

  for (int ipage = 0; ipage < npage_kv; ipage++) {
    nkey_kv = kv->request_page(ipage,kdummy,vdummy,adummy);
 
    ptr = page_kv;

    for (int i = 0; i < nkey_kv; i++) {
      keybytes = *((int *) ptr);
      valuebytes = *((int *) (ptr+sizeof(int)));;

      ptr += twolenbytes;
      ptr = ROUNDUP(ptr,kalignm1);
      key = ptr;
      ptr += keybytes;
      ptr = ROUNDUP(ptr,valignm1);
      value = ptr;
      ptr += valuebytes;
      ptr = ROUNDUP(ptr,talignm1);

      add(key,keybytes,value,valuebytes);
    }
  }
}

/* ----------------------------------------------------------------------
   collapse a KV into a KMV with one KMV pair
   new KMV key = key, new KMV multivalue = key,value,key,value,etc from KV
   each processor works on only its data
   called by MR::collapse()
------------------------------------------------------------------------- */

void KeyMultiValue::collapse(char *key, int keybytes, KeyValue *kv)
{
  // check if collapsed KV pairs fit in one KMV page

  int nkey_kv = kv->nkv;
  int ksize_kv = kv->ksize;
  int vsize_kv = kv->vsize;
  
  int totalsize = (3 + nkey_kv) * sizeof(int);
  totalsize = roundup(totalsize,kalign);
  totalsize += keybytes;
  totalsize = roundup(totalsize,valign);
  totalsize += ksize_kv + vsize_kv;
  totalsize = roundup(totalsize,talign);

  if (totalsize > pagesize) 
    error->one("Single KMV pair exceeds KeyMultiValue page size");
    
  // create memory layout for one large KMV

  int *intptr = (int *) page;
  *(intptr++) = keybytes;
  *(intptr++) = ksize_kv + vsize_kv;
  *(intptr++) = 2*nkey_kv;
  int *valuesizes = intptr;
  intptr += 2*nkey_kv;

  char *cptr = ROUNDUP((char *) intptr,kalignm1);
  memcpy(cptr,key,keybytes);
  cptr += keybytes;
  char *multivalue = ROUNDUP(cptr,valignm1);

  // loop over KV, copying its keys/values into KMV valuesizes/multivalue

  int kdummy,vdummy,adummy;
  int valuebytes;

  char *page_kv;
  int npage_kv = kv->request_info(&page_kv);

  int keybytes_kv,valuebytes_kv;
  char *ptr,*key_kv,*value_kv;
  int ivalue = 0;

  for (int ipage = 0; ipage < npage_kv; ipage++) {
    nkey_kv = kv->request_page(ipage,kdummy,vdummy,adummy);

    ptr = page_kv;

    for (int i = 0; i < nkey_kv; i++) {
      keybytes_kv = *((int *) ptr);
      valuebytes_kv = *((int *) (ptr+sizeof(int)));;

      ptr += twolenbytes;
      ptr = ROUNDUP(ptr,kalignm1);
      key_kv = ptr;
      ptr += keybytes_kv;
      ptr = ROUNDUP(ptr,valignm1);
      value_kv = ptr;
      ptr += valuebytes_kv;
      ptr = ROUNDUP(ptr,talignm1);

      valuesizes[ivalue++] = keybytes_kv;
      memcpy(multivalue,key_kv,keybytes_kv);
      multivalue += keybytes_kv;

      valuesizes[ivalue++] = valuebytes_kv;
      memcpy(multivalue,value_kv,valuebytes_kv);
      multivalue += valuebytes_kv;
    }
  }

  nkey = 1;
  nvalue = 2*nkey_kv;
  keysize = keybytes;
  valuesize = ksize_kv + vsize_kv;
  alignsize = totalsize;
}

/* ----------------------------------------------------------------------
   convert a KV with non-unique keys into a KMV with unique keys
   each processor works on only its data
   called by MR::convert()
------------------------------------------------------------------------- */

void KeyMultiValue::convert(KeyValue *kv, char *memunique, int memsize)
{
  int i,ichunk,spoolflag,spooled,fcount,nnew,nbits;
  char sfile[32];

  maxpartition = PARTITIONCHUNK;
  partitions = (Partition *) 
    memory->smalloc(maxpartition*sizeof(Partition),"KMV:partitions");

  maxset = SETCHUNK;
  sets = (Set *) memory->smalloc(maxset*sizeof(Set),"KMV:sets");

  memspool = SPOOLMBYTES * 1024*1024;
  chunks = NULL;
  nchunk = 0;

  fcount = 0;

  // partition memunique to hold unique keys
  // each unique key requires:
  //   1 entry in Unique, 1 bucket (rounded down), keyave bytes in ukeys
  // maxunique = max # of unique keys that can be held in uniques
  // nbuckets = power of 2 just smaller than maxunique
  // maxukeys = max size of all keys in ukeys

  int nkv = kv->nkv;
  int ksize_kv = kv->ksize;
  double keyave = 1.0*ksize_kv/nkv;

  maxunique = 
    static_cast<int> (memsize / (sizeof(Unique) + sizeof(int) + keyave));
  if (maxunique == 0) error->one("Cannot hold any unique keys in memory");

  nbuckets = 1;
  while (nbuckets <= maxunique) nbuckets *= 2;
  nbuckets /= 2;
  hashmask = nbuckets-1;

  int offset = 0;
  uniques = (Unique *) &memunique[offset];
  offset += maxunique*sizeof(Unique);
  buckets = (int *) &memunique[offset];
  offset += nbuckets*sizeof(int);
  ukeys = (char *) &memunique[offset];
  maxukeys = memsize-offset;

  // loop over partitions of KV pairs
  // 1 partition = portion of KV pairs whose unique keys fit in memory
  // partitions that exceed this induce splitting of KV to spool files
  // first parittion is entire KV which may create more partitions

  npartition = 1;
  partitions[0].nkv = nkv;
  partitions[0].ksize = ksize_kv;
  partitions[0].sortbit = 0;
  partitions[0].kv = kv;
  partitions[0].sp = NULL;
  int ipartition = 0;

  while (ipartition < npartition) {

    // spoolflag = 0 if KV keys all fit in memory even if all unique
    // spoolflag = 1 if may need to split into seen & unseen spools

    spoolflag = 0;
    if (partitions[ipartition].nkv > maxunique || 
	partitions[ipartition].ksize > maxukeys) {
      spoolflag = 1;

      sprintf(sfile,"mrmpi.sp.%d.%d",fcount++,me);
      seen = new Spool(sfile,memspool,memory,error);
      sprintf(sfile,"mrmpi.sp.%d.%d",fcount++,me);
      unseen = new Spool(sfile,memspool,memory,error);
      seen_ksize = unseen_ksize = 0;
    }

    // assign memory to spool files

    ichunk = 0;
    if (partitions[ipartition].sp) ichunk++;
    if (spoolflag) ichunk += 2;
    chunk_allocate(ichunk);
    ichunk = 0;
    if (partitions[ipartition].sp) 
      partitions[ipartition].sp->assign(chunks[ichunk++]);
    if (spoolflag) {
      seen->assign(chunks[ichunk++]);
      unseen->assign(chunks[ichunk++]);
    }

    // loop over partition or KV pairs to populate list of unique keys
    // optionally spool KV pairs to seen/unseen spool files

    spooled = kv2unique(ipartition,spoolflag);

    // if not used, delete seen and unseen spools
    // will happen if unique keys for paritition fit in memory
    
    if (spoolflag && spooled == 0) {
      delete seen;
      delete unseen;
    }

    // spooling occurred so need to split partition KVs
    // spool file "seen" becomes spool file for 1st subset
    // spool file "unseen" is split into spool files for new partitions
    // estimate nnew = # of needed new partitions by seen/unseen file sizes
    // nnew = next larger power-of-2 so can use hash bits for splitting unseen

    if (spooled) {
      delete partitions[ipartition].sp;

      nnew = unseen->nkv/seen->nkv + 1;
      nbits = 0;
      while ((1 << nbits) < nnew) nbits++;
      nnew = 1 << nbits;

      // DEBUG
      //printf("NNEW %d %d %d: %d %d %p\n",ipartition,
      //	     nnew,partitions[ipartition].sortbit,
      //     unseen->nkv,seen->nkv,seen);

      if (npartition+nnew >= maxpartition) {
	while (maxpartition < npartition+nnew) maxpartition += PARTITIONCHUNK;
	partitions = (Partition *) 
	  memory->srealloc(partitions,maxpartition*sizeof(Partition),
			   "KMV:partitions");
      }

      partitions[ipartition].nkv = seen->nkv;
      partitions[ipartition].ksize = seen_ksize;;
      partitions[ipartition].kv = NULL;
      partitions[ipartition].sp = seen;

      // if only one new partition, unseen becomes its KV source
      // else scan unseen spool file and create additional spool files

      if (nnew == 1) {
	// DEBUG
	// printf("  ASSIGN %d %d\n",npartition,unseen->nkv);
      	partitions[npartition].nkv = unseen->nkv;
      	partitions[npartition].ksize = unseen_ksize;
      	partitions[npartition].sortbit = partitions[ipartition].sortbit;
      	partitions[npartition].kv = NULL;
      	partitions[npartition].sp = unseen;
      	npartition++;

      } else {
	chunk_allocate(nnew+1);
	ichunk = 0;
	unseen->assign(chunks[ichunk++]);

	for (i = npartition; i < npartition+nnew; i++) {
	  partitions[i].kv = NULL;
	  sprintf(sfile,"mrmpi.sp.%d.%d",fcount++,me);
	  partitions[i].sp = new Spool(sfile,memspool,memory,error);
	  partitions[i].sp->assign(chunks[ichunk++]);
	}
	
	unseen2spools(nnew,nbits,partitions[ipartition].sortbit);
	delete unseen;
	npartition += nnew;
      }
    }

    // split this partition's unique keys into sets
    // 1 set = portion of uniques which map to single KMV page
    // first pass thru entire uniques may create more sets and further looping
    // multiple sets induce further splitting of partition to sub-spool files

    nset = 1;
    sets[0].first = 0;
    sets[0].last = nunique-1;
    sets[0].kv = partitions[ipartition].kv;
    sets[0].sp = partitions[ipartition].sp;

    for (int iset = 0; iset < nset; iset++) {

      // loop over set of unique keys to structure a KMV page
      // for iset = 0, unique2kmv may reset nset if multiple pages triggered

      spooled = unique2kmv(iset);

      // multiple KMV pages induced by scan of 1st set = all uniques
      // split partition KVs into sub-spools, one per set = KMV page

      if (spooled) {
	chunk_allocate(nset+1);
	ichunk = 0;
	if (partitions[ipartition].sp)
	  partitions[ipartition].sp->assign(chunks[ichunk++]);

	for (i = 0; i < nset; i++) {
	  sets[i].kv = NULL;
	  sprintf(sfile,"mrmpi.sp.%d.%d",fcount++,me);
	  sets[i].sp = new Spool(sfile,memspool,memory,error);
	  sets[i].sp->assign(chunks[ichunk++]);
	}

	unique2spools(ipartition);
	delete partitions[ipartition].sp;
      }

      // scan KV pairs in a set to populate one KMV page with KV values

      if (sets[iset].sp) sets[iset].sp->assign(chunks[0]);
      kv2kmv(iset);
      delete sets[iset].sp;

      // write KMV page to disk unless very last one
      
      if (iset < nset-1 || ipartition < npartition-1) {
	// DEBUG
	//if (me == 1) printf("CALL to write page %d %d %d: %d %d %d %d\n",
	//		    npage,nkey,alignsize,iset,nset,
	//		    ipartition,npartition);
	create_page();
	write_page();
	npage++;
      }
    }

    ipartition++;
  }
  
  // clean up

  memory->sfree(partitions);
  memory->sfree(sets);
  for (i = 0; i < nchunk; i++) memory->sfree(chunks[i]);
  memory->sfree(chunks);
}

/* ----------------------------------------------------------------------
   scan KV pairs to populate list of unique keys
   KV pairs can come from entire KV or from a spool file
   if spoolflag set, write pairs out to 2 spool files
   if spooling, tally stats on unseen keys
   return 1 if unseed keys were spooled, else 0
------------------------------------------------------------------------- */

int KeyMultiValue::kv2unique(int ipartition, int spoolflag)
{
  int i,itally,kdummy,vdummy,adummy;
  int nkey_kv,keybytes,valuebytes,ibucket,ikey,last;
  char *ptr,*ptr_start,*key;

  nunique = 0;
  for (i = 0; i < nbuckets; i++) buckets[i] = -1;
  ukeyoffset = 0;

  int spooled = 0;

  // scan a KV partition, one page at a time, to produce list of unique keys
  // partition can be original KV or a spool file
  // hash KV key, find it in unique list
  // either add to unique list or increment cummulative multivalue size
  // spool unique keys into seen spool
  // spool keys that won't fit in memory into unseen spool

  KeyValue *kv = partitions[ipartition].kv;
  Spool *sp = partitions[ipartition].sp;
  
  int npage_kv;
  char *page_kv;
  if (kv) npage_kv = kv->request_info(&page_kv);
  else npage_kv = sp->request_info(&page_kv);

  for (int ipage = 0; ipage < npage_kv; ipage++) {
    if (kv) nkey_kv = kv->request_page(ipage,kdummy,vdummy,adummy);
    else nkey_kv = sp->request_page(ipage);

    ptr = page_kv;
    
    for (i = 0; i < nkey_kv; i++) {
      keybytes = *((int *) ptr);
      valuebytes = *((int *) (ptr+sizeof(int)));;
      
      ptr_start = ptr;
      ptr += twolenbytes;
      ptr = ROUNDUP(ptr,kalignm1);
      key = ptr;
      ptr += keybytes;
      ptr = ROUNDUP(ptr,valignm1);
      ptr += valuebytes;
      ptr = ROUNDUP(ptr,talignm1);

      ibucket = hash(key,keybytes);
      ikey = find(ibucket,key,keybytes,last);

      // if key is new unique:
      //   if in-memory unique keys are filled, start spooling
      //   else add key to unique list
      // else if not new unique: increment counters

      if (ikey < 0) {
	if (spooled || 
	    nunique == maxunique || ukeyoffset+keybytes > maxukeys) {
	  if (spoolflag == 0) error->one("Internal error in kv2unique");
	  spooled = 1;
	  unseen->add(ptr-ptr_start,ptr_start);
	  unseen_ksize += keybytes;

	} else {
	  if (last < 0) buckets[ibucket] = nunique;
	  else uniques[last].next = nunique;

	  if (spoolflag) seen->add(ptr-ptr_start,ptr_start);
	  seen_ksize += keybytes;

	  uniques[nunique].keyoffset = ukeyoffset;
	  memcpy(&ukeys[ukeyoffset],key,keybytes);
	  ukeyoffset += keybytes;
	  uniques[nunique].keybytes = keybytes;
	
	  uniques[nunique].nvalue = 1;
	  uniques[nunique].mvbytes = valuebytes;
	  uniques[nunique].next = -1;
	  nunique++;
	}

      } else {
	if (spoolflag) seen->add(ptr-ptr_start,ptr_start);
	seen_ksize += keybytes;

	uniques[ikey].nvalue++;
	uniques[ikey].mvbytes += valuebytes;
      }
    }
  }

  if (spooled) {
    seen->complete();
    unseen->complete();
  }

  return spooled;
}

/* ----------------------------------------------------------------------
   split spool file of unseen keys into one spool file per partition
   nnew = # of new partitions = 2^nbits
   new partitions are from Npartition to Npartition + nnew
------------------------------------------------------------------------- */

void KeyMultiValue::unseen2spools(int nnew, int nbits, int sortbit)
{
  int i,nentry,keybytes,valuebytes,ispool;
  uint32_t ubucket;
  char *ptr,*ptr_start,*key;

  // construct bitmask on key hash to split into equal subsets
  // use nbits beyond sortbit of unseen KVs

  int mask = nnew-1;
  int shift = 32 - nbits - sortbit;

  // target spools

  Spool **spools = new Spool*[nnew];
  for (i = 0; i < nnew; i++) spools[i] = partitions[npartition+i].sp;

  int *ksize = new int[nnew];
  for (i = 0; i < nnew; i++) ksize[i] = 0;

  // loop over KV pairs in unseen list
  // ispool = which spool file to write it to

  char *page_kv;
  int npage_kv = unseen->request_info(&page_kv);

  for (int ipage = 0; ipage < npage_kv; ipage++) {
    nentry = unseen->request_page(ipage);

    ptr = page_kv;

    for (i = 0; i < nentry; i++) {
      ptr_start = ptr;
      keybytes = *((int *) ptr);
      valuebytes = *((int *) (ptr+sizeof(int)));;

      ptr += twolenbytes;
      ptr = ROUNDUP(ptr,kalignm1);
      key = ptr;
      ptr += keybytes;
      ptr = ROUNDUP(ptr,valignm1);
      ptr += valuebytes;
      ptr = ROUNDUP(ptr,talignm1);

      ubucket = hashlittle(key,keybytes,0);
      ispool = (ubucket >> shift) & mask;

      if (ispool < 0 || ispool >= nnew)
	error->one("Internal error in unseen2spools");

      spools[ispool]->add(ptr-ptr_start,ptr_start);
      ksize[ispool] += keybytes;
    }
  }

  for (i = 0; i < nnew; i++) spools[i]->complete();

  // set new partition info

  for (i = 0; i < nnew; i++) {
    partitions[npartition+i].nkv = spools[i]->nkv;
    partitions[npartition+i].ksize = ksize[i];
    partitions[npartition+i].sortbit = sortbit + nbits;
    // DEBUG
    // if (me == 1) printf("  PFILE %d %d\n",npartition+i,spools[i]->nkv);
  }

  delete [] spools;
  delete [] ksize;
}

/* ----------------------------------------------------------------------
   scan subset of unique keys to structure a single KMV page
   scan of set 0 is over entire list of unique keys
   if mapping exceeds one KMV page:
     only structure the first page
     nset = # of pages, redo first/last bounds
   return 1 if multiple pages induced, else 0
------------------------------------------------------------------------- */

int KeyMultiValue::unique2kmv(int iset)
{
  char *iptr,*nvptr,*kptr,*vptr;
  int *intptr;

  int spooled = 0;

  // counts for current KMV page

  nkey = nvalue = keysize = valuesize = 0;

  // loop over subset of unique keys
  // when spooling, store index of spool file in uniques[].nvalue
  // this works b/c uniques[].nvalue = 0 for 1st page

  char *ptr = page;
  int first = sets[iset].first;
  int last = sets[iset].last;
  int totalsize = 0;

  for (int i = first; i <= last; i++) {
    iptr = ptr;
    nvptr = iptr + threelenbytes;
    kptr = nvptr + uniques[i].nvalue*sizeof(int);
    kptr = ROUNDUP(kptr,kalignm1);
    vptr = kptr + uniques[i].keybytes;
    vptr = ROUNDUP(vptr,valignm1);
    ptr = vptr + uniques[i].mvbytes;
    ptr = ROUNDUP(ptr,talignm1);
    totalsize += ptr - iptr;
    
    if (totalsize > pagesize) {
      spooled = 1;
      if (iset) error->one("Internal error in unique2kmv");
      if (nset == 1) alignsize = totalsize - (ptr-iptr);
      totalsize = ptr - iptr;
      if (totalsize > pagesize) {
	printf("KMV size: %d %d %d\n",
	       uniques[i].keybytes,uniques[i].mvbytes,uniques[i].nvalue);
	error->one("Single KMV pair exceeds KeyMultiValue page size");
      }

      sets[nset-1].last = i-1;
      if (nset == maxset) {
	maxset += SETCHUNK;
	sets = (Set *) memory->srealloc(sets,maxset*sizeof(Set),"KMV:sets");
      }
      sets[nset++].first = i;
      uniques[i].iset = nset-1;

    } else if (spooled == 0) {
      nkey++;
      nvalue += uniques[i].nvalue;
      keysize += uniques[i].keybytes;
      valuesize += uniques[i].mvbytes;
      
      intptr = (int *) iptr;
      *(intptr++) = uniques[i].keybytes;
      *(intptr++) = uniques[i].mvbytes;
      *(intptr++) = uniques[i].nvalue;

      memcpy(kptr,&ukeys[uniques[i].keyoffset],uniques[i].keybytes);

      uniques[i].soffset = nvptr - page;
      uniques[i].voffset = vptr - page;
      uniques[i].nvalue = 0;
      uniques[i].mvbytes = 0;
      uniques[i].iset = 0;

    } else uniques[i].iset = nset-1;
  }

  if (spooled == 0) alignsize = totalsize;
  sets[nset-1].last = nunique-1;

  return spooled;
}

/* ----------------------------------------------------------------------
   split partition file (KV or spool) into one spool file per set (KMV page)
------------------------------------------------------------------------- */

void KeyMultiValue::unique2spools(int ipartition)
{
  int i,nentry,keybytes,valuebytes,ibucket,ikey,ispool,dummy;
  int nkey_kv,kdummy,vdummy,adummy;
  char *ptr,*ptr_start,*key;

  Spool **spools = new Spool*[nset];
  for (i = 0; i < nset; i++) spools[i] = sets[i].sp;

  // loop over KV pairs in this partition
  // uniques[].nvalue stores index of spool file for each unique key

  KeyValue *kv = partitions[ipartition].kv;
  Spool *sp = partitions[ipartition].sp;

  int npage_kv;
  char *page_kv;
  if (kv) npage_kv = kv->request_info(&page_kv);
  else npage_kv = sp->request_info(&page_kv);

  for (int ipage = 0; ipage < npage_kv; ipage++) {
    if (kv) nkey_kv = kv->request_page(ipage,kdummy,vdummy,adummy);
    else nkey_kv = sp->request_page(ipage);

    ptr = page_kv;

    for (i = 0; i < nkey_kv; i++) {
      ptr_start = ptr;
      keybytes = *((int *) ptr);
      valuebytes = *((int *) (ptr+sizeof(int)));;

      ptr += twolenbytes;
      ptr = ROUNDUP(ptr,kalignm1);
      key = ptr;
      ptr += keybytes;
      ptr = ROUNDUP(ptr,valignm1);
      ptr += valuebytes;
      ptr = ROUNDUP(ptr,talignm1);

      ibucket = hash(key,keybytes);
      ikey = find(ibucket,key,keybytes,dummy);
      ispool = uniques[ikey].iset;

      if (ispool < 0 || ispool >= nset)
	error->one("Internal error in unique2spools");

      spools[ispool]->add(ptr-ptr_start,ptr_start);
    }
  }

  for (i = 0; i < nset; i++) spools[i]->complete();
  delete [] spools;
}

/* ----------------------------------------------------------------------
   scan KV pairs to populate single KMV page with values
------------------------------------------------------------------------- */

void KeyMultiValue::kv2kmv(int iset)
{
  int i,nkey_kv,keybytes,valuebytes,ibucket,ikey,dummy;
  int kdummy,vdummy,adummy;
  char *ptr,*key,*value,*multivalue;
  int *valuesizes;

  KeyValue *kv = sets[iset].kv;
  Spool *sp = sets[iset].sp;

  int npage_kv;
  char *page_kv;
  if (kv) npage_kv = kv->request_info(&page_kv);
  else npage_kv = sp->request_info(&page_kv);

  for (int ipage = 0; ipage < npage_kv; ipage++) {
    if (kv) nkey_kv = kv->request_page(ipage,kdummy,vdummy,adummy);
    else nkey_kv = sp->request_page(ipage);
	
    ptr = page_kv;
	
    for (i = 0; i < nkey_kv; i++) {
      keybytes = *((int *) ptr);
      valuebytes = *((int *) (ptr+sizeof(int)));;
	  
      ptr += twolenbytes;
      ptr = ROUNDUP(ptr,kalignm1);
      key = ptr;
      ptr += keybytes;
      ptr = ROUNDUP(ptr,valignm1);
      value = ptr;
      ptr += valuebytes;
      ptr = ROUNDUP(ptr,talignm1);
	  
      ibucket = hash(key,keybytes);
      ikey = find(ibucket,key,keybytes,dummy);
      
      if (ikey < 0)
	error->one("Internal error in kv2kmv");

      valuesizes = (int *) &page[uniques[ikey].soffset];
      valuesizes[uniques[ikey].nvalue++] = valuebytes;
      multivalue = &page[uniques[ikey].voffset];
      memcpy(&multivalue[uniques[ikey].mvbytes],value,valuebytes);
      uniques[ikey].mvbytes += valuebytes;
    }
  }
}

/* ----------------------------------------------------------------------
   allocate chunks of memory for Spool files
------------------------------------------------------------------------- */

void KeyMultiValue::chunk_allocate(int n)
{
  if (n <= nchunk) return;
  chunks = (char **) memory->srealloc(chunks,n*sizeof(char *),"KMV:chunks");
  for (int i = nchunk; i < n; i++) {
    chunks[i] = (char *) memory->smalloc(memspool,"KMV:chunk");
    memset(chunks[i],0,memspool);
  }
  nchunk = n;
}

/* ----------------------------------------------------------------------
   find a Unique in ibucket that matches key
   return index of Unique
   if cannot find key, return -1
   also set last = -1 if bucket was empty, last = if not found, also update bucket and next ptrs for key to be added
------------------------------------------------------------------------- */

int KeyMultiValue::find(int ibucket, char *key, int keybytes, int &last)
{
  int ikey = buckets[ibucket];
  if (ikey < 0) {
    last = -1;
    return -1;
  }

  int offset,next;
  while (ikey >= 0) {
    offset = uniques[ikey].keyoffset;
    if (keybytes == uniques[ikey].keybytes && 
	strncmp(key,&ukeys[offset],keybytes) == 0) return ikey;
    last = ikey;
    ikey = uniques[ikey].next;
  }

  return -1;
}

/* ----------------------------------------------------------------------
   hash a key to a bucket
------------------------------------------------------------------------- */

int KeyMultiValue::hash(char *key, int keybytes)
{
  uint32_t ubucket = hashlittle(key,keybytes,0);
  int ibucket = ubucket & hashmask;
  return ibucket;
}

/* ----------------------------------------------------------------------
   create virtual page entry for in-memory page
------------------------------------------------------------------------- */

void KeyMultiValue::init_page()
{
  nkey = nvalue = 0;
  keysize = valuesize = 0;
  alignsize = 0;
}

/* ----------------------------------------------------------------------
   create virtual page entry for in-memory page
------------------------------------------------------------------------- */

void KeyMultiValue::create_page()
{
  if (npage == maxpage) {
    maxpage += PAGECHUNK;
    pages = (Page *) memory->srealloc(pages,maxpage*sizeof(Page),"KV:pages");
  }

  pages[npage].nkey = nkey;
  pages[npage].keysize = keysize;
  pages[npage].valuesize = valuesize;
  pages[npage].exactsize =
    3*nkey*sizeof(int) + nvalue*sizeof(int) + keysize + valuesize;
  pages[npage].alignsize = alignsize;
  pages[npage].filesize = roundup(alignsize,ALIGNFILE);

  if (npage)
    pages[npage].fileoffset = 
      pages[npage-1].fileoffset + pages[npage-1].filesize;
  else
    pages[npage].fileoffset = 0;
}

/* ----------------------------------------------------------------------
   write in-memory page to disk
   do a seek since may be overwriting a previous partial page
------------------------------------------------------------------------- */

void KeyMultiValue::write_page()
{
  if (fp == NULL) {
    fp = fopen(filename,"wb");
    if (fp == NULL) 
      error->one("Could not open KeyMultiValue file for writing");
    fileflag = 1;
  }

  long fileoffset = pages[npage].fileoffset;
  fseek(fp,fileoffset,SEEK_SET);
  fwrite(page,pages[npage].filesize,1,fp);
}

/* ----------------------------------------------------------------------
   read ipage from disk
   do a seek since may be reading last page
------------------------------------------------------------------------- */

void KeyMultiValue::read_page(int ipage, int writeflag)
{
  if (fp == NULL) {
    if (writeflag) fp = fopen(filename,"r+b");
    else fp = fopen(filename,"rb");
    if (fp == NULL) 
      error->one("Could not open KeyMultiValue file for reading");
  }

  long fileoffset = pages[ipage].fileoffset;
  fseek(fp,fileoffset,SEEK_SET);
  fread(page,pages[ipage].filesize,1,fp);
}

/* ----------------------------------------------------------------------
   round N up to multiple of nalign and return it
------------------------------------------------------------------------- */

int KeyMultiValue::roundup(int n, int nalign)
{
  if (n % nalign == 0) return n;
  n = (n/nalign + 1) * nalign;
  return n;
}
