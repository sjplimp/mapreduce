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

// allocate space for static class variables and initialize them

uint64_t KeyMultiValue::rsize = 0;
uint64_t KeyMultiValue::wsize = 0;

#define MIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

#define ROUNDUP(A,B) (char *) (((uint64_t) A + B) & ~B);

#define ALIGNFILE 512              // same as in mapreduce.cpp
#define PARTITIONCHUNK 1
#define SETCHUNK 1
#define PAGECHUNK 16
#define SPOOLMBYTES 1
#define INTMAX 0x7FFFFFFF

/* ---------------------------------------------------------------------- */

KeyMultiValue::KeyMultiValue(MPI_Comm comm_caller,
			     char *memblock, uint64_t memsize,
			     int memkalign, int memvalign, char *memfile)
{
  comm = comm_caller;
  MPI_Comm_rank(comm,&me);

  memory = new Memory(comm);
  error = new Error(comm);

  int n = strlen(memfile) + 1;
  filename = new char[n];
  strcpy(filename,memfile);
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
  ualign = sizeof(uint64_t);

  kalignm1 = kalign-1;
  valignm1 = valign-1;
  talignm1 = talign-1;
  ualignm1 = ualign-1;

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
  delete [] filename;
}

/* ----------------------------------------------------------------------
   reset KMV page to another chunk of memory
   done by caller when it is manipulating memory
------------------------------------------------------------------------- */

void KeyMultiValue::reset_page(char *memblock)
{
  page = memblock;
}

/* ----------------------------------------------------------------------
   copy contents of another KMV into me
   input KMV should never be self
------------------------------------------------------------------------- */

void KeyMultiValue::copy(KeyMultiValue *kmv)
{
  if (kmv == this) error->all("Cannot perform KeyMultiValue copy on self");

  // pages will be loaded into other KMV's memory
  // temporarily set my page to other KMV's page of memory
  // write_page() then writes them from that page to my spool file

  char *page_hold = page;
  int npage_other = kmv->request_info(&page);

  for (int ipage = 0; ipage < npage_other-1; ipage++) {
    nkey = kmv->request_page(ipage,0,keysize,valuesize,alignsize);
    create_page();
    write_page();
    npage++;
  }

  // last page needs to be copied to my memory before calling complete()
  // reset my page to my memory

  nkey = kmv->request_page(npage_other-1,0,keysize,valuesize,alignsize);
  memcpy(page_hold,page,alignsize);
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

int KeyMultiValue::request_page(int ipage, int writeflag, 
				uint64_t &keysize_page, 
				uint64_t &valuesize_page,
				uint64_t &alignsize_page)
{
  // load page from file if necessary

  if (fileflag) read_page(ipage,writeflag);

  keysize_page = pages[ipage].keysize;
  valuesize_page = pages[ipage].valuesize;
  alignsize_page = pages[ipage].alignsize;

  return pages[ipage].nkey;
}

/* ----------------------------------------------------------------------
   return # of values in a multi-page KMV
   also return # of blocks of values on subsequent pages
------------------------------------------------------------------------- */

uint64_t KeyMultiValue::multivalue_blocks(int ipage, int &nblock)
{
  nblock = pages[ipage].nblock;
  return pages[ipage].nvalue_total;
}

/* ----------------------------------------------------------------------
   write out a changed page of KMV data
   called by MR::sort_multivalues()
------------------------------------------------------------------------- */

void KeyMultiValue::overwrite_page(int ipage)
{
  if (!fileflag) return;
  write_page();
}

/* ----------------------------------------------------------------------
   close disk file if open
   called by MR::compress() and MR::reduce()
------------------------------------------------------------------------- */

void KeyMultiValue::close_file()
{
  if (fp) {
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
  char *kptr = iptr + threelenbytes + sizeof(int);
  kptr = ROUNDUP(kptr,kalignm1);
  char *vptr = kptr + keybytes;
  vptr = ROUNDUP(vptr,valignm1);
  char *nptr = vptr + valuebytes;
  nptr = ROUNDUP(nptr,talignm1);
  int kmvbytes = nptr - iptr;
  
  // page is full, write to disk
  
  if (alignsize + kmvbytes > pagesize || nkey == INTMAX) {
    if (alignsize == 0) {
      printf("KeyMultiValue pair size/limit: %d %u\n",kmvbytes,pagesize);
      error->one("Single key/multivalue pair exceeds page size");
    }

    create_page();
    write_page();
    npage++;
    init_page();
    add(key,keybytes,value,valuebytes);
    return;
  }

  int *intptr = (int *) iptr;
  *(intptr++) = 1;
  *(intptr++) = keybytes;
  *(intptr++) = valuebytes;
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

  int nkey_kv,keybytes,valuebytes;
  uint64_t kdummy,vdummy,adummy;
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
   collapse a KV into a single KMV pair
   new KMV key = key, new KMV multivalue = key,value,key,value,etc from KV
   each processor works on only its data
   called by MR::collapse()
------------------------------------------------------------------------- */

void KeyMultiValue::collapse(char *key, int keybytes, KeyValue *kv)
{
  // check if new KMV fits in one page
  
  uint64_t nkey_kv = kv->nkv;
  uint64_t ksize_kv = kv->ksize;
  uint64_t vsize_kv = kv->vsize;

  uint64_t totalsize = threelenbytes + 2*nkey_kv*sizeof(int);
  totalsize = roundup(totalsize,kalign);
  totalsize += keybytes;
  totalsize = roundup(totalsize,valign);
  totalsize += ksize_kv + vsize_kv;
  totalsize = roundup(totalsize,talign);

  if (2*nkey_kv <= INTMAX && ksize_kv+vsize_kv <= INTMAX &&
      totalsize <= pagesize)
    collapse_one(key,keybytes,kv,totalsize);
  else collapse_many(key,keybytes,kv);
}

/* ----------------------------------------------------------------------
   create a single KMV page from a collapsed KV
   called by KMV::collapse()
------------------------------------------------------------------------- */

void KeyMultiValue::collapse_one(char *key, int keybytes, KeyValue *kv,
				 uint64_t totalsize)
{
  // create memory layout for one large KMV

  int *iptr = (int *) page;
  *(iptr++) = 2*kv->nkv;
  *(iptr++) = keybytes;
  *(iptr++) = kv->ksize + kv->vsize;
  int *valuesizes = iptr;
  iptr += 2*kv->nkv;
  char *cptr = ROUNDUP((char *) iptr,kalignm1);
  memcpy(cptr,key,keybytes);
  cptr += keybytes;
  char *multivalue = ROUNDUP(cptr,valignm1);

  // loop over KV, copying its keys/values into KMV valuesizes/multivalue

  char *page_kv;
  int npage_kv = kv->request_info(&page_kv);

  int nkey_kv,keybytes_kv,valuebytes_kv;
  uint64_t kdummy,vdummy,adummy;
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
  nvalue = 2*kv->nkv;
  keysize = keybytes;
  valuesize = kv->ksize + kv->vsize;
  alignsize = totalsize;
}

/* ----------------------------------------------------------------------
   create multiple KMV pages from a collapsed KV
   called by KMV::collapse()
------------------------------------------------------------------------- */

void KeyMultiValue::collapse_many(char *key, int keybytes, KeyValue *kv)
{
  error->all("Collapse of multi-block KMV not yet supported");
}

/* ----------------------------------------------------------------------
   convert a KV with non-unique keys into a KMV with unique keys
   each processor works on only its data
   called by MR::convert()
------------------------------------------------------------------------- */

void KeyMultiValue::convert(KeyValue *kv, char *memunique, uint64_t memsize,
			    char *fpath)
{
  int i,ichunk,spoolflag,spooled,nnew,nbits;

  int n = strlen(fpath) + 32;
  char *sfile = new char[n];
  int fcount = 0;

  // setup partition, set, and chunk data structs

  maxpartition = PARTITIONCHUNK;
  partitions = (Partition *) 
    memory->smalloc(maxpartition*sizeof(Partition),"KMV:partitions");

  maxset = SETCHUNK;
  sets = (Set *) memory->smalloc(maxset*sizeof(Set),"KMV:sets");

  memspool = SPOOLMBYTES * 1024*1024;
  chunks = NULL;
  nchunk = 0;

  // estimate = # of unique keys that can be stored in memunique
  // each unique key requires roughly:
  //   1 Unique, 1 hash bucket, keyave bytes for the key itself
  // set nbuckets to power of 2 just smaller than estimate
  // also limit nbuckets to INTMAX+1 = 2^31
  //   since are using 32-bit hash and want hashmask & ibucket to be ints
  // set aside first portion of memunique for nbuckets
  // remainder for Unique data structs + keys

  double keyave = 1.0*kv->ksize/kv->nkv;
  double oneave = keyave + sizeof(Unique) + sizeof(Unique *);
  uint64_t estimate = static_cast<uint64_t> (memsize/oneave);
  if (estimate == 0) error->one("Cannot hold any unique keys in memory");

  uint64_t nbuckets = 1;
  while (nbuckets <= estimate) nbuckets *= 2;
  nbuckets /= 2;
  nbuckets = MIN(nbuckets,INTMAX);
  if (nbuckets == INTMAX) nbuckets++;
  hashmask = nbuckets-1;

  buckets = (Unique **) memunique;
  bucketbytes = nbuckets*sizeof(Unique *);
  ustart = memunique + bucketbytes;
  ustop = memunique + memsize;
  ukeyoffset = sizeof(Unique);

  if (ustop-ustart < ukeyoffset)
    error->one("Cannot hold any unique keys in memory");

  // loop over partitions of KV pairs
  // 1 partition = portion of KV pairs whose unique keys fit in memory
  // partitions that exceed this induce splitting of KV to spool files
  // first partition is entire KV which may create more partitions

  npartition = 1;
  partitions[0].nkv = kv->nkv;
  partitions[0].ksize = kv->ksize;
  partitions[0].sortbit = 0;
  partitions[0].kv = kv;
  partitions[0].sp = NULL;
  int ipartition = 0;

  while (ipartition < npartition) {

    // spoolflag = 0 if KV keys all fit in memory even if all unique
    // spoolflag = 1 if may need to split into seen & unseen spools
    // one KV key requires nbytes = these quantities:
    //   1 unique = ukeyoffset
    //   key size = keybytes
    //   aligment factor = ualignm1 at most

    spoolflag = 0;
    if (partitions[ipartition].nkv*(ukeyoffset+ualignm1) + 
	partitions[ipartition].ksize > ustop-ustart) {
      spoolflag = 1;

      sprintf(sfile,"%s/mrmpi.spl.%d.%d",fpath,fcount++,me);
      seen = new Spool(sfile,memspool,memory,error);
      sprintf(sfile,"%s/mrmpi.spl.%d.%d",fpath,fcount++,me);
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

    // if spooling didn't occur, delete seen and unseen spools
    // will be the case if unique keys for partition fit in memory
    
    if (spoolflag && spooled == 0) {
      delete seen;
      delete unseen;
    }

    // spooling occurred so need to create partitions of KVs
    // spool file "seen" becomes spool file for 1st subset
    // spool file "unseen" is split into spool files for new partitions
    // estimate nnew = # of needed new partitions by seen/unseen file sizes
    // nnew = next larger power-of-2 so can use hash bits for splitting unseen

    if (spooled) {
      if (partitions[ipartition].sp) delete partitions[ipartition].sp;

      nnew = unseen->nkv/seen->nkv + 1;
      nbits = 0;
      while ((1 << nbits) < nnew) nbits++;
      nnew = 1 << nbits;

      if (npartition+nnew >= maxpartition) {
	while (maxpartition < npartition+nnew) maxpartition += PARTITIONCHUNK;
	partitions = (Partition *) 
	  memory->srealloc(partitions,maxpartition*sizeof(Partition),
			   "KMV:partitions");
      }

      partitions[ipartition].kv = NULL;
      partitions[ipartition].sp = seen;
      partitions[ipartition].nkv = seen->nkv;
      partitions[ipartition].ksize = seen_ksize;;

      // if only one new partition, unseen becomes its KV source
      // else scan unseen spool file and create additional spool files

      if (nnew == 1) {
      	partitions[npartition].kv = NULL;
      	partitions[npartition].sp = unseen;
      	partitions[npartition].nkv = unseen->nkv;
      	partitions[npartition].ksize = unseen_ksize;
      	partitions[npartition].sortbit = partitions[ipartition].sortbit;
      	npartition++;

      } else {
	chunk_allocate(nnew+1);
	ichunk = 0;
	unseen->assign(chunks[ichunk++]);

	for (i = npartition; i < npartition+nnew; i++) {
	  partitions[i].kv = NULL;
	  sprintf(sfile,"%s/mrmpi.spl.%d.%d",fpath,fcount++,me);
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
    sets[0].kv = partitions[ipartition].kv;
    sets[0].sp = partitions[ipartition].sp;

    for (int iset = 0; iset < nset; iset++) {

      // loop over unique keys to structure KMV pages
      // if iset = 0:
      //   loop over all unique keys and create nsets and sets data struct
      //   flag each set as extended if its last KMV is a multi-page KMV
      //   structure the one KMV page for iset = 0
      // if iset > 0:
      //   loop over its unique keys to structure one KMV page

      spooled = 0;
      if (iset == 0) spooled = unique2kmv_all();
      if (sets[iset].extended) unique2kmv_extended(iset);
      else if (iset > 0) unique2kmv_set(iset);

      // multiple KMV pages were induced by scan of all unique keys
      // split KV partition into one sub-spool per set via unique2spools

      if (spooled) {
	chunk_allocate(nset+1);
	ichunk = 0;
	if (partitions[ipartition].sp)
	  partitions[ipartition].sp->assign(chunks[ichunk++]);

	for (i = 0; i < nset; i++) {
	  sets[i].kv = NULL;
	  sprintf(sfile,"%s/mrmpi.spl.%d.%d",fpath,fcount++,me);
	  sets[i].sp = new Spool(sfile,memspool,memory,error);
	  sets[i].sp->assign(chunks[ichunk++]);
	}

	unique2spools(ipartition);
	if (partitions[ipartition].sp) delete partitions[ipartition].sp;
      }

      // scan KV pairs in a set to populate KMV page(s) with KV values
      // it last KMV pair in set is extended, multiple pages will be output
      
      if (sets[iset].sp) sets[iset].sp->assign(chunks[0]);
      if (!sets[iset].extended) kv2kmv(iset);
      else kv2kmv_extended(iset);
      if (sets[iset].sp) delete sets[iset].sp;

      // write KMV page to disk unless very last one
      
      if (iset < nset-1 || ipartition < npartition-1) {
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
  delete [] sfile;
}

/* ----------------------------------------------------------------------
   scan KV pairs to populate list of unique keys
   KV pairs can come from entire KV or from a spool file
   if spoolflag set, write pairs out to 2 spool files
   if spooling, tally stats on unseen keys
   return 1 if unseen keys were spooled, else 0
------------------------------------------------------------------------- */

int KeyMultiValue::kv2unique(int ipartition, int spoolflag)
{
  int nkey_kv,keybytes,valuebytes,ibucket;
  uint64_t kdummy,vdummy,adummy;
  char *ptr,*ptr_start,*key,*keyunique,*unext;
  Unique *uptr,*uprev;

  nunique = 0;
  unext = ustart;
  memset(buckets,0,bucketbytes);

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
    
    for (int i = 0; i < nkey_kv; i++) {
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
      uptr = find(ibucket,key,keybytes,uprev);

      // if key is in unique list, increment counters
      // else if already spooling, add to unseen spool
      // else if no memory for another unique key, start spooling to unseen
      // else add key to unique list

      if (uptr) {
	if (spoolflag) {
	  seen->add(ptr-ptr_start,ptr_start);
	  seen_ksize += keybytes;
	}
	uptr->nvalue++;
	uptr->mvbytes += valuebytes;

      } else if (spooled) {
	unseen->add(ptr-ptr_start,ptr_start);
	unseen_ksize += keybytes;
	
      } else {
	uptr = (Unique *) unext;
	unext += ukeyoffset + keybytes;
	unext = ROUNDUP(unext,ualignm1);

	if (unext > ustop) {
	  spooled = 1;
	  unseen->add(ptr-ptr_start,ptr_start);
	  unseen_ksize += keybytes;
	  continue;
	}

	if (spoolflag) {
	  seen->add(ptr-ptr_start,ptr_start);
	  seen_ksize += keybytes;
	}

	if (uprev) uprev->next = uptr;
	else buckets[ibucket] = uptr;
	
	uptr->nvalue = 1;
	uptr->mvbytes = valuebytes;
	uptr->next = NULL;
	uptr->keybytes = keybytes;
	keyunique = ((char *) uptr) + ukeyoffset;
	memcpy(keyunique,key,keybytes);
	nunique++;
      }
    }
  }

  if (spoolflag) seen->complete();
  if (spooled) unseen->complete();

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

  uint64_t *sp_ksize = new uint64_t[nnew];
  for (i = 0; i < nnew; i++) sp_ksize[i] = 0;

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
      sp_ksize[ispool] += keybytes;
    }
  }

  for (i = 0; i < nnew; i++) spools[i]->complete();

  // set new partition info

  for (i = 0; i < nnew; i++) {
    partitions[npartition+i].nkv = spools[i]->nkv;
    partitions[npartition+i].ksize = sp_ksize[i];
    partitions[npartition+i].sortbit = sortbit + nbits;
  }

  delete [] spools;
  delete [] sp_ksize;
}

/* ----------------------------------------------------------------------
   scan all unique keys to identify subsets to spool
   determine nset = # of subsets, nset = 1 initially
     each set stores first Unique and number of uniques in set
   if one KMV pair exceeds single page, flag its set as extended
   also structure the first KMV page for set 0 unless it is extended
   return 1 if multiple sets induced, else 0
------------------------------------------------------------------------- */

int KeyMultiValue::unique2kmv_all()
{
  int multiflag,newflag;
  uint64_t onesize,setsize;
  char *ptr,*iptr,*nvptr,*kptr,*vptr,*keyunique,*unext;
  int *intptr;
  Unique *uptr;

  // counts for first KMV page for set 0

  nkey = 0;
  nvalue = keysize = valuesize = 0;
  ptr = page;

  // loop over all unique keys
  // when forced to spool, store index of spool file in uptr->set

  uptr = (Unique *) ustart;
  newflag = 1;

  for (int i = 0; i < nunique; i++) {
    if (newflag) {
      newflag = 0;
      sets[nset-1].first = uptr;
      sets[nset-1].nunique = i;
      sets[nset-1].extended = 0;
      setsize = 0;
    }
    
    iptr = ptr;
    nvptr = iptr + threelenbytes;
    kptr = nvptr + uptr->nvalue*sizeof(int);
    kptr = ROUNDUP(kptr,kalignm1);
    vptr = kptr + uptr->keybytes;
    vptr = ROUNDUP(vptr,valignm1);
    ptr = vptr + uptr->mvbytes;
    ptr = ROUNDUP(ptr,talignm1);
    onesize = ptr - iptr;

    // test if KMV pair is a single-page or multi-page KMV
    // is multi-page if:
    //   onesize exceeds page size, nvalues or mvbytes exceed INTMAX

    multiflag = 0;
    if (onesize > pagesize || uptr->nvalue > INTMAX || uptr->mvbytes > INTMAX)
      multiflag = 1;

    // single-page KMV pair
    // if space remains in page and nkey < INTMAX, add it to this set
    // else close set and add it to new set
    // if added to set 0:
    //   induce structure on first KMV page and modify unique info accordingly

    if (multiflag == 0) {
      if (setsize + onesize > pagesize || nkey == INTMAX) {
	sets[nset-1].nunique = i - sets[nset-1].nunique;
	if (nset == 1) alignsize = setsize;

	if (nset == maxset) {
	  maxset += SETCHUNK;
	  sets = (Set *) memory->srealloc(sets,maxset*sizeof(Set),"KMV:sets");
	}
	nset++;

	sets[nset-1].first = uptr;
	sets[nset-1].nunique = i;
	sets[nset-1].extended = 0;
	setsize = 0;
      }

      uptr->set = nset-1;
      setsize += onesize;

      if (nset == 1) {
	intptr = (int *) iptr;
	*(intptr++) = uptr->nvalue;
	*(intptr++) = uptr->keybytes;
	*(intptr++) = uptr->mvbytes;
	keyunique = ((char *) uptr) + ukeyoffset;
	memcpy(kptr,keyunique,uptr->keybytes);

	nkey++;
	nvalue += uptr->nvalue;
	keysize += uptr->keybytes;
	valuesize += uptr->mvbytes;

	uptr->soffset = (int *) nvptr;
	uptr->voffset = vptr;
	uptr->nvalue = 0;
	uptr->mvbytes = 0;
      }

    // multi-page KMV pair
    // if current set is just this KMV, close it as extended set
    // else close it as regular set, add new set as extended set
    // set newflag if more uniques exist, so new set will be initialized

    } else {
      if (uptr == sets[nset-1].first) {
	sets[nset-1].nunique = 1;
	sets[nset-1].extended = 1;

      } else {
	sets[nset-1].nunique = i - sets[nset-1].nunique;
	if (nset == 1) alignsize = setsize;

	if (nset == maxset) {
	  maxset += SETCHUNK;
	  sets = (Set *) memory->srealloc(sets,maxset*sizeof(Set),"KMV:sets");
	}
	nset++;

	sets[nset-1].first = uptr;
	sets[nset-1].nunique = 1;
	sets[nset-1].extended = 1;
      }

      uptr->set = nset-1;

      if (i < nunique-1) {
	if (nset == maxset) {
	  maxset += SETCHUNK;
	  sets = (Set *) memory->srealloc(sets,maxset*sizeof(Set),"KMV:sets");
	}
	nset++;
	newflag = 1;
      }
    }

    // set uptr to next Unique

    unext = (char *) uptr;
    unext += ukeyoffset + uptr->keybytes;
    unext = ROUNDUP(unext,ualignm1);
    uptr = (Unique *) unext;
  }

  if (sets[nset-1].extended == 0) {
    sets[nset-1].nunique = nunique - sets[nset-1].nunique;
    if (nset == 1) alignsize = setsize;
  }

  if (nset == 1) return 0;
  return 1;
}

/* ----------------------------------------------------------------------
   structure a KMV page from a set with a single extended KMV pair
   only header page is created here
   content pages will be created in kv2kmv_extended()
------------------------------------------------------------------------- */

void KeyMultiValue::unique2kmv_extended(int iset)
{
  char *ptr,*iptr,*kptr,*keyunique;
  int *intptr;
  Unique *uptr;

  ptr = page;
  uptr = sets[iset].first;

  iptr = ptr;
  kptr = iptr + twolenbytes;
  kptr = ROUNDUP(kptr,kalignm1);
  ptr = kptr + uptr->keybytes;

  intptr = (int *) iptr;
  *(intptr++) = 0;
  *(intptr++) = uptr->keybytes;
  keyunique = ((char *) uptr) + ukeyoffset;
  memcpy(kptr,keyunique,uptr->keybytes);

  nkey = 1;
  nvalue = uptr->nvalue;
  keysize = uptr->keybytes;
  valuesize = uptr->mvbytes;
  alignsize = ptr - iptr;
}

/* ----------------------------------------------------------------------
   scan subset of unique keys to structure a single KMV page
------------------------------------------------------------------------- */

void KeyMultiValue::unique2kmv_set(int iset)
{
  char *ptr,*iptr,*nvptr,*kptr,*vptr,*keyunique,*unext;
  int *intptr;
  Unique *uptr;

  // counts for current KMV page

  nkey = 0;
  nvalue = keysize = valuesize = 0;
  uint64_t setsize = 0;

  // loop over subset of unique keys

  ptr = page;
  uptr = sets[iset].first;
  int n = sets[iset].nunique;

  for (int i = 0; i < n; i++) {
    iptr = ptr;
    nvptr = iptr + threelenbytes;
    kptr = nvptr + uptr->nvalue*sizeof(int);
    kptr = ROUNDUP(kptr,kalignm1);
    vptr = kptr + uptr->keybytes;
    vptr = ROUNDUP(vptr,valignm1);
    ptr = vptr + uptr->mvbytes;
    ptr = ROUNDUP(ptr,talignm1);
    setsize += ptr - iptr;

    intptr = (int *) iptr;
    *(intptr++) = uptr->nvalue;
    *(intptr++) = uptr->keybytes;
    *(intptr++) = uptr->mvbytes;
    keyunique = ((char *) uptr) + ukeyoffset;
    memcpy(kptr,keyunique,uptr->keybytes);

    nkey++;
    nvalue += uptr->nvalue;
    keysize += uptr->keybytes;
    valuesize += uptr->mvbytes;
    
    uptr->soffset = (int *) nvptr;
    uptr->voffset = vptr;
    uptr->nvalue = 0;
    uptr->mvbytes = 0;

    unext = (char *) uptr;
    unext += ukeyoffset + uptr->keybytes;
    unext = ROUNDUP(unext,ualignm1);
    uptr = (Unique *) unext;
  }

  alignsize = setsize;
}

/* ----------------------------------------------------------------------
   split partition file (KV or spool) into one spool file per set (KMV page)
------------------------------------------------------------------------- */

void KeyMultiValue::unique2spools(int ipartition)
{
  int i,nentry;
  int nkey_kv,keybytes,valuebytes,ibucket,ispool;
  uint64_t kdummy,vdummy,adummy;
  char *ptr,*ptr_start,*key;
  Unique *uptr,*udummy;

  Spool **spools = new Spool*[nset];
  for (i = 0; i < nset; i++) spools[i] = sets[i].sp;

  // loop over KV pairs in this partition
  // uptr->set stores index of spool file for each unique key

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
      uptr = find(ibucket,key,keybytes,udummy);
      if (!uptr) error->one("Internal find error in unique2spools");

      ispool = uptr->set;
      if (ispool < 0 || ispool >= nset)
	error->one("Internal spool error in unique2spools");
      spools[ispool]->add(ptr-ptr_start,ptr_start);
    }
  }

  for (i = 0; i < nset; i++) spools[i]->complete();
  delete [] spools;
}

/* ----------------------------------------------------------------------
   scan KV pairs to populate single KMV page with values
   iset contains many KMV pairs
------------------------------------------------------------------------- */

void KeyMultiValue::kv2kmv(int iset)
{
  int nkey_kv,keybytes,valuebytes,ibucket;
  uint64_t kdummy,vdummy,adummy;
  char *ptr,*key,*value,*multivalue;
  int *valuesizes;
  Unique *uptr,*udummy;

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
	  
      ibucket = hash(key,keybytes);
      uptr = find(ibucket,key,keybytes,udummy);
      if (!uptr) error->one("Internal find error in kv2kmv");

      valuesizes = uptr->soffset;
      valuesizes[uptr->nvalue++] = valuebytes;
      multivalue = uptr->voffset;
      memcpy(&multivalue[uptr->mvbytes],value,valuebytes);
      uptr->mvbytes += valuebytes;
    }
  }
}

/* ----------------------------------------------------------------------
   scan KV pairs to populate multiple KMV pages with values
   iset contains one multi-page KMV pair
   first write out header page created by unique2kmv_extended()
   write out all value pages except last one which caller will write
   when done, must set nblock count in header page
------------------------------------------------------------------------- */

void KeyMultiValue::kv2kmv_extended(int iset)
{
  int nkey_kv,keybytes,valuebytes;
  uint64_t kdummy,vdummy,adummy;
  char *ptr,*key,*value,*vptr;

  // write out header page

  create_page();
  write_page();

  Unique *uptr = sets[iset].first;
  pages[npage].nvalue_total = uptr->nvalue;

  int header_page = npage;
  npage++;

  // zero counters for value pages
  // header page stored counts for entire KMV pair

  nkey = 0;
  nvalue = keysize = valuesize = 0;

  // split KMV page into two half-pages for valuesizes and values
  // maxvalue = max # of values the first half can hold
  // leave leading int in first half for nvalue count

  uint64_t halfsize = pagesize/2;
  int maxvalue = MIN(INTMAX,halfsize/sizeof(int)-1);
  int *valuesizes = (int *) &page[sizeof(int)];
  char *multivalue = &page[halfsize];
  char *valuestart = multivalue;
  char *valuestop = page + pagesize;

  // loop over KV pairs, all with same key
  // add value info to two half-pages
  // write out page when when either half-page is full

  int nblock = 0;
  int ncount = 0;

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

      // if either half-page exceeded, pack two halves together, write page
      // use memmove() since target may overlap source

      if (ncount == maxvalue || multivalue+valuebytes > valuestop) {
	if (ncount == 0) {
	  printf("Value size/limit: %d %u\n",valuebytes,halfsize);
	  error->one("Single value exceeds multi-page KMV page size");
	}

	*((int *) page) = ncount;
	vptr = page + ((uint64_t) ncount)*sizeof(int) + sizeof(int);
	vptr = ROUNDUP(vptr,valignm1);
	memmove(vptr,valuestart,multivalue-valuestart);
	vptr += multivalue - valuestart;
	alignsize = vptr - page;

	create_page();
	write_page();
	npage++;

	nblock++;
	ncount = 0;
	multivalue = valuestart;
      }

      memcpy(multivalue,value,valuebytes);
      multivalue += valuebytes;
      valuesizes[ncount++] = valuebytes;
    }
  }

  // setup last partially filled page
  // will be written by caller

  *((int *) page) = ncount;
  vptr = page + ((uint64_t) ncount)*sizeof(int) + sizeof(int);
  vptr = ROUNDUP(vptr,valignm1);
  memmove(vptr,valuestart,multivalue-valuestart);
  vptr += multivalue - valuestart;
  alignsize = vptr - page;

  nblock++;

  // set nblock count in header page

  pages[header_page].nblock = nblock;
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
   if cannot find key, return NULL
   if bucket was empty, set prev = NULL
   else set prev = ptr to last Unique in the bucket
------------------------------------------------------------------------- */

KeyMultiValue::Unique *KeyMultiValue::find(int ibucket, char *key, 
					   int keybytes, Unique *&uprev)
{
  Unique *uptr = buckets[ibucket];
  if (!uptr) {
    uprev = NULL;
    return NULL;
  }

  char *keyunique;
  while (uptr) {
    keyunique = ((char *) uptr) + ukeyoffset;
    if (keybytes == uptr->keybytes && memcmp(key,keyunique,keybytes) == 0)
      return uptr;
    uprev = uptr;
    uptr = uptr->next;
  }

  return NULL;
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
  pages[npage].exactsize = ((uint64_t) nkey)*threelenbytes + 
    nvalue*sizeof(int) + keysize + valuesize;
  pages[npage].alignsize = alignsize;
  pages[npage].filesize = roundup(alignsize,ALIGNFILE);
  pages[npage].nvalue_total = 0;
  pages[npage].nblock = 0;

  if (npage)
    pages[npage].fileoffset = 
      pages[npage-1].fileoffset + pages[npage-1].filesize;
  else
    pages[npage].fileoffset = 0;
}

/* ----------------------------------------------------------------------
   write in-memory page to disk
   do a seek since may be overwriting a previous page for extended KMV
------------------------------------------------------------------------- */

void KeyMultiValue::write_page()
{
  if (fp == NULL) {
    fp = fopen(filename,"wb");
    if (fp == NULL) 
      error->one("Could not open KeyMultiValue file for writing");
    fileflag = 1;
  }

  uint64_t fileoffset = pages[npage].fileoffset;
  fseek(fp,fileoffset,SEEK_SET);
  fwrite(page,pages[npage].filesize,1,fp);
  wsize += pages[npage].filesize;
}

/* ----------------------------------------------------------------------
   read ipage from disk
   do a seek since may be reading arbitrary page for extended KMV
------------------------------------------------------------------------- */

void KeyMultiValue::read_page(int ipage, int writeflag)
{
  if (fp == NULL) {
    if (writeflag) fp = fopen(filename,"r+b");
    else fp = fopen(filename,"rb");
    if (fp == NULL) 
      error->one("Could not open KeyMultiValue file for reading");
  }

  uint64_t fileoffset = pages[ipage].fileoffset;
  fseek(fp,fileoffset,SEEK_SET);
  fread(page,pages[ipage].filesize,1,fp);
  rsize += pages[ipage].filesize;
}

/* ----------------------------------------------------------------------
   round N up to multiple of nalign and return it
------------------------------------------------------------------------- */

uint64_t KeyMultiValue::roundup(uint64_t n, int nalign)
{
  if (n % nalign == 0) return n;
  n = (n/nalign + 1) * nalign;
  return n;
}
