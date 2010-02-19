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
#include "mapreduce.h"
#include "keyvalue.h"
#include "spool.h"
#include "hash.h"
#include "memory.h"
#include "error.h"

using namespace MAPREDUCE_NS;

// allocate space for static class variables and initialize them

uint64_t KeyMultiValue::rsize = 0;
uint64_t KeyMultiValue::wsize = 0;
double KeyMultiValue::rtime = 0.0;
double KeyMultiValue::wtime = 0.0;

#define MIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

#define ROUNDUP(A,B) (char *) (((uint64_t) A + B) & ~B);

#define ALIGNFILE 512              // same as in mapreduce.cpp
#define PARTITIONCHUNK 16
#define SETCHUNK 16
#define PAGECHUNK 16
#define SPOOLCHUNK 16
#define SPOOLMBYTES 1
#define INTMAX 0x7FFFFFFF

/* ---------------------------------------------------------------------- */

KeyMultiValue::KeyMultiValue(MapReduce *mr_caller, 
			     int memkalign, int memvalign,
			     Memory *memory_caller, Error *error_caller,
			     MPI_Comm comm_caller)
{
  mr = mr_caller;
  memory = memory_caller;
  error = error_caller;
  comm = comm_caller;
  MPI_Comm_rank(comm,&me);

  filename = mr->file_create(1);
  fileflag = 0;
  fp = NULL;

  pages = NULL;
  npage = maxpage = 0;

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

  nkmv = ksize = vsize = esize = 0;
  init_page();
}

/* ---------------------------------------------------------------------- */

KeyMultiValue::~KeyMultiValue()
{
  memory->sfree(pages);
  if (fileflag) remove(filename);
  delete [] filename;
}

/* ----------------------------------------------------------------------
   trigger KMV to request an available page of memory
------------------------------------------------------------------------- */

void KeyMultiValue::set_page()
{
  page = mr->mymalloc(1,pagesize,memtag);
}

/* ----------------------------------------------------------------------
   copy contents of another KMV into me
   input KMV should never be self
   input KMV will have same pagesize and alignment as me
   called by MR::copy()
------------------------------------------------------------------------- */

void KeyMultiValue::copy(KeyMultiValue *kmv)
{
  if (kmv == this) error->all("Cannot perform KeyMultiValue copy on self");

  // pages will be loaded into memory assigned to other KMV
  // write_page() will write them from that page to my file

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
  complete(0);
  page = page_hold;
}

/* ----------------------------------------------------------------------
   complete the KMV after data has been added to it
   called by MR methods after creating & populating a KMV
   forceflag = 1 if want to force KV to be written to disk
------------------------------------------------------------------------- */

void KeyMultiValue::complete(int forceflag)
{
  create_page();

  // if disk file exists, write last page, close file

  if (fileflag || forceflag) {
    write_page();
    fclose(fp);
    fp = NULL;
  }

  npage++;
  init_page();

  // set sizes for entire KMV

  nkmv = ksize = vsize = esize = 0;
  for (int ipage = 0; ipage < npage; ipage++) {
    nkmv += pages[ipage].nkey;
    ksize += pages[ipage].keysize;
    vsize += pages[ipage].valuesize;
    esize += pages[ipage].exactsize;
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

void KeyMultiValue::convert(KeyValue *kv)
{
  // setup partition, set, and chunk data structs

  maxpartition = PARTITIONCHUNK;
  partitions = (Partition *) 
    memory->smalloc(maxpartition*sizeof(Partition),"KMV:partitions");

  maxset = SETCHUNK;
  sets = (Set *) memory->smalloc(maxset*sizeof(Set),"KMV:sets");

  memspool = SPOOLMBYTES * 1024*1024;
  chunks = NULL;
  nchunk = maxchunk = 0;

  // estimate = # of unique keys that can be stored in 2 pages of memunique
  // each unique key requires roughly:
  //   1 Unique, 1 hash bucket, keyave bytes for the key itself
  // set nbuckets to power of 2 just smaller than estimate
  // also limit nbuckets to INTMAX+1 = 2^31
  //   since are using 32-bit hash and want hashmask & ibucket to be ints
  // set aside first portion of memunique for nbuckets
  // remainder for Unique data structs + keys

  uint64_t uniquesize;
  int uniquetag;
  char *memunique = mr->mymalloc(2,uniquesize,uniquetag);

  double keyave = 1.0*kv->ksize/kv->nkv;
  double oneave = keyave + sizeof(Unique) + sizeof(Unique *);
  uint64_t estimate = static_cast<uint64_t> (uniquesize/oneave);
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
  ustop = memunique + uniquesize;
  ukeyoffset = sizeof(Unique);

  if (ustop-ustart < ukeyoffset)
    error->one("Cannot hold any unique keys in memory");

  // loop over partitions of KV pairs
  // partition = portion of KV pairs whose unique keys fit in memory
  // first partition is entire KV which may be split into more partitions

  npartition = 1;
  partitions[0].kv = kv;
  partitions[0].sp = NULL;
  partitions[0].sp2 = NULL;
  partitions[0].sortbit = 0;
  int ipartition = 0;

  while (ipartition < npartition) {

    printf("PARTITION %d of %d on proc %d\n",ipartition,npartition,me);

    // scan KVs for unique keys, split partition if too big

    kv2unique(ipartition);

    // loop over sets of unique keys
    // set = portion of uniques which map to single KMV page
    // first set is entire partition which may be split into more sets

    nset = 1;
    sets[0].kv = partitions[ipartition].kv;
    sets[0].sp = partitions[ipartition].sp;
    sets[0].sp2 = partitions[ipartition].sp2;

    for (int iset = 0; iset < nset; iset++) {

      printf("  SET %d of %d on proc %d\n",iset,nset,me);

      // scan unique keys to structure KMV pages
      // if iset = 0:
      //   loop over all unique keys and create nsets
      //   flag each set as extended if it stores a multi-page KMV
      //   structure KMV page for iset = 0
      // if iset > 0:
      //   loop over its unique keys to structure one KMV page
      //   different operation for single-page vs multi-page KMV pairs

      int split = 0;
      if (iset == 0) split = unique2kmv_all();
      if (sets[iset].extended) unique2kmv_extended(iset);
      else if (iset > 0) unique2kmv_set(iset);

      printf("    SET results: %d split, %d extended\n",
	     split,sets[iset].extended);

      // if multiple KMV pages were induced by unique2kmv_all()
      // then split partition into one Spool per set

      if (split) partition2sets(ipartition);

      // scan KV pairs in set to populate a single KMV page
      // if set is extended, multiple pages will be output
      
      if (sets[iset].extended) kv2kmv_extended(iset);
      else kv2kmv(iset);

      // write KMV page to disk unless very last one
      
      if (iset < nset-1 || ipartition < npartition-1) {
	create_page();
	write_page();
	npage++;
      }
    }

    // free Spools and memory chunks for all sets and one partition
    // if nset = 1, then set 0 has Spools from ipartition, so don't re-delete
    // if partition has original KV,
    // then delete it to recover disk space and memory page

    for (int iset = 0; iset < nset; iset++) {
      if (sets[iset].sp) {
	nchunk--;
	delete sets[iset].sp;
      }
      if (sets[iset].sp2) {
	nchunk--;
	delete sets[iset].sp2;
      }
    }

    if (nset > 1 && partitions[ipartition].sp) {
      nchunk--;
      delete partitions[ipartition].sp;
    }
    if (nset > 1 && partitions[ipartition].sp2) {
      nchunk--;
      delete partitions[ipartition].sp2;
    }
    if (partitions[ipartition].kv) {
      mr->myfree(kv->memtag);
      delete kv;
    }

    ipartition++;
  }
  
  // clean up memory

  mr->myfree(uniquetag);
  memory->sfree(partitions);
  memory->sfree(sets);
  for (int i = 0; i < maxchunk; i++) memory->sfree(chunks[i]);
  memory->sfree(chunks);
}

/* ----------------------------------------------------------------------
   scan KV pairs in a partition to populate list of unique keys
   create new partitions if KV pairs overflow Unique list
------------------------------------------------------------------------- */

void KeyMultiValue::kv2unique(int ipartition)
{
  int nkey_kv,keybytes,valuebytes,ibucket,pagecut,ncut;
  int nnew,nbits,mask,shift,ispool;
  uint64_t kdummy,vdummy,adummy,sizecut;
  uint32_t ubucket;
  char *ptr,*ptr_start,*key,*keyunique,*unext;
  Unique *uptr,*uprev;
  Spool *spextra;
  Spool **spools;

  int full = 0;
  uint64_t count = 0;

  nunique = 0;
  unext = ustart;
  memset(buckets,0,bucketbytes);

  // loop over KV pairs in this partition
  // source of KV pairs is either full KV or a Spool, not both
  // hash key for each KV pair, find it in unique list
  // either add to unique list or increment cummulative multivalue size

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
      count++;

      // if key is already in unique list, increment counters
      // if unique list is full, write KV to extra Spool

      if (uptr) {
	uptr->nvalue++;
	uptr->mvbytes += valuebytes;
	if (full) spextra->add(ptr-ptr_start,ptr_start);
	continue;
      }

      // if space available, add key to unique list

      uptr = (Unique *) unext;
      unext += ukeyoffset + keybytes;
      unext = ROUNDUP(unext,ualignm1);

      if (unext <= ustop) {
	if (uprev) uprev->next = uptr;
	else buckets[ibucket] = uptr;
	
	uptr->nvalue = 1;
	uptr->mvbytes = valuebytes;
	uptr->next = NULL;
	uptr->keybytes = keybytes;
	keyunique = ((char *) uptr) + ukeyoffset;
	memcpy(keyunique,key,keybytes);
	nunique++;
	continue;
      }

      // space is not available, so overflow into new Spool files
      // if this is first overflow KV pair, create partitions
      // pagecut,ncut,sizecut = info on last KV pair before cut
      // nnew = estimate of # of new parts based on KV fraction seen so far
      // nnew = next larger power-of-2, so can use hash bits to split
      // mask = bitmask on key hash to split into equal subsets
      // use nbits beyond sortbit of current partition

      if (full == 0) {
	full = 1;
	pagecut = ipage;
	ncut = i;
	sizecut = ptr_start - page_kv;

	count--;
	if (kv) nnew = (kv->nkv-count)/count + 1;
	else nnew = (sp->nkv-count)/count + 1;
	nbits = 0;
	while ((1 << nbits) < nnew) nbits++;
	nnew = 1 << nbits;
	if (kv)
	  printf("PARTITION split: %u count, %u total, %d nbits, %d nnew\n",
		 count,kv->nkv,nbits,nnew);
	else
	  printf("PARTITION split: %u count, %u total, %d nbits, %d nnew\n",
		 count,sp->nkv,nbits,nnew);

	mask = nnew-1;
	shift = 32 - partitions[ipartition].sortbit - nbits;

	spextra = augment_partition(ipartition);
	spools = new Spool*[nnew];
	for (int j = 0; j < nnew; j++) {
	  spools[j] = create_partition();
	  partitions[npartition-1].sortbit = 
	    partitions[ipartition].sortbit + nbits;
	}
      }

      // add KV pair to appropriate partition

      ubucket = hashlittle(key,keybytes,0);
      ispool = (ubucket >> shift) & mask;
      spools[ispool]->add(ptr-ptr_start,ptr_start);
    }
  }

  // truncate KV or Spool source file of KV pairs using cut info
  // force all new Spool files to close and write to disk

  if (full) {
    if (kv) kv->truncate(pagecut,ncut,sizecut);
    else sp->truncate(pagecut,ncut,sizecut);
    spextra->complete();
    for (int j = 0; j < nnew; j++) spools[j]->complete();
    delete [] spools;
  }
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
  // create new set when page size exceeded by single- or multi-page KMV pairs
  // uptr->set = assignment of key to set

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
    // else close set and add it as first KMV to new set
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
   split partition of KVs into one Spool file per set (KMV page)
------------------------------------------------------------------------- */

void KeyMultiValue::partition2sets(int ipartition)
{
  int i,twosource;
  int nkey_kv,keybytes,valuebytes,ibucket,ispool;
  uint64_t kdummy,vdummy,adummy;
  char *ptr,*ptr_start,*key;
  Unique *uptr,*udummy;

  Spool **spools = new Spool*[nset];
  for (i = 0; i < nset; i++) {
    Spool *sp = new Spool(mr,memory,error);
    sp->set_page(memspool,chunk_allocate());
    spools[i] = sets[i].sp = sp;
  }

  // loop over KV pairs in this partition
  // source of KV pairs can be a KV, KV + Spool, Spool, or Spool + Spool2
  // uptr->set stores set index for each unique key

  KeyValue *kv = partitions[ipartition].kv;
  Spool *sp = partitions[ipartition].sp;
  Spool *sp2 = partitions[ipartition].sp2;

  twosource = 0;
  if (kv && sp) twosource = 1;
  if (sp && sp2) twosource = 1;

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
      if (!uptr) error->one("Internal find error in partition2sets");

      ispool = uptr->set;
      if (ispool < 0 || ispool >= nset)
	error->one("Internal spool error in partition2sets");
      spools[ispool]->add(ptr-ptr_start,ptr_start);
    }

    // switch KV source from KV to Spool or Spool to Spool2

    if (ipage == npage_kv-1 && twosource) {
      twosource = 0;
      ipage = 0;
      if (kv) {
	npage_kv = sp->request_info(&page_kv);
	kv = NULL;
      } else {
	npage_kv = sp2->request_info(&page_kv);
	sp = sp2;
      }
    }
  }

  for (i = 0; i < nset; i++) spools[i]->complete();
  delete [] spools;
}

/* ----------------------------------------------------------------------
   scan KV pairs in a set to populate single KMV page with values
   iset and KMV page contain many KMV pairs
------------------------------------------------------------------------- */

void KeyMultiValue::kv2kmv(int iset)
{
  int nkey_kv,keybytes,valuebytes,ibucket,twosource;
  uint64_t kdummy,vdummy,adummy;
  char *ptr,*key,*value,*multivalue;
  int *valuesizes;
  Unique *uptr,*udummy;

  // loop over KV pairs in this set
  // source of KV pairs can be KV, KV + Spool, Spool, or Spool + Spool2

  KeyValue *kv = sets[iset].kv;
  Spool *sp = sets[iset].sp;
  Spool *sp2 = sets[iset].sp2;

  twosource = 0;
  if (kv && sp) twosource = 1;
  if (sp && sp2) twosource = 1;

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

    // switch KV source from KV to Spool or Spool to Spool2

    if (ipage == npage_kv-1 && twosource) {
      twosource = 0;
      ipage = 0;
      if (kv) {
	npage_kv = sp->request_info(&page_kv);
	kv = NULL;
      } else {
	npage_kv = sp2->request_info(&page_kv);
	sp = sp2;
      }
    }
  }
}

/* ----------------------------------------------------------------------
   scan KV pairs to populate multiple KMV pages with values
   iset and KMV pages contain one multi-page KMV pair
   first write out header page created by unique2kmv_extended()
   write out all value pages except last one which caller will write
   when done, set nblock count in header page
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

  int npage_kv;
  char *page_kv;
  npage_kv = kv->request_info(&page_kv);

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
   augment a partition with a Spool to store overflow KV pairs
   if partition has a KV, sp stores new Spool
   if partition has a Spool, sp2 stores new Spool
------------------------------------------------------------------------- */

Spool *KeyMultiValue::augment_partition(int ipartition)
{
  Spool *sp = new Spool(mr,memory,error);
  sp->set_page(memspool,chunk_allocate());
  if (partitions[ipartition].kv) {
    partitions[ipartition].sp = sp;
    partitions[ipartition].sp2 = NULL;
  } else partitions[ipartition].sp2 = sp;
  return sp;
}

/* ----------------------------------------------------------------------
   create a new partition with a Spool to store overflow KV pairs
------------------------------------------------------------------------- */

Spool *KeyMultiValue::create_partition()
{
  if (npartition == maxpartition) {
    maxpartition += PARTITIONCHUNK;
    partitions = (Partition *) 
      memory->srealloc(partitions,maxpartition*sizeof(Partition),
		       "KMV:partitions");
  }

  Spool *sp = new Spool(mr,memory,error);
  sp->set_page(memspool,chunk_allocate());
  partitions[npartition].kv = NULL;
  partitions[npartition].sp = sp;
  partitions[npartition].sp2 = NULL;
  npartition++;
  return sp;
}

/* ----------------------------------------------------------------------
   allocate chunk of memory for a new Spool file
------------------------------------------------------------------------- */

char *KeyMultiValue::chunk_allocate()
{
  if (nchunk == maxchunk) {
    maxchunk += SPOOLCHUNK;
    chunks = (char **) 
      memory->srealloc(chunks,maxchunk*sizeof(char *),"KMV:chunks");
    for (int i = nchunk; i < maxchunk; i++) {
      chunks[i] = (char *) memory->smalloc(memspool,"KMV:chunk");
      memset(chunks[i],0,memspool);
    }
  }

  nchunk++;
  return chunks[nchunk-1];
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
   do a seek since may be overwriting an arbitrary page due to sort
------------------------------------------------------------------------- */

void KeyMultiValue::write_page()
{
  if (fp == NULL) {
    fp = fopen(filename,"wb");
    if (fp == NULL) 
      error->one("Could not open KeyMultiValue file for writing");
    fileflag = 1;
  }

  double timestart = MPI_Wtime();
  uint64_t fileoffset = pages[npage].fileoffset;
  fseek(fp,fileoffset,SEEK_SET);
  fwrite(page,pages[npage].filesize,1,fp);
  wtime += MPI_Wtime() - timestart;
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

  double timestart = MPI_Wtime();
  uint64_t fileoffset = pages[ipage].fileoffset;
  fseek(fp,fileoffset,SEEK_SET);
  fread(page,pages[ipage].filesize,1,fp);
  rtime += MPI_Wtime() - timestart;
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
