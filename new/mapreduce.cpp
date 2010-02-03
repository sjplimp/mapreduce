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
#include "ctype.h"
#include "string.h"
#include "stdio.h"
#include "stdlib.h"
#include "stdint.h"
#include "sys/types.h"
#include "sys/stat.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "keymultivalue.h"
#include "irregular.h"
#include "spool.h"
#include "hash.h"
#include "memory.h"
#include "error.h"

using namespace MAPREDUCE_NS;

// allocate space for static class variables and initialize them

MapReduce *MapReduce::mrptr;
int MapReduce::instances_now = 0;
int MapReduce::instances_ever = 0;
int MapReduce::mpi_finalize_flag = 0;
uint64_t MapReduce::cssize = 0;
uint64_t MapReduce::crsize = 0;

// prototypes for non-class functions

void map_file_standalone(int, KeyValue *, void *);
int compare_standalone(const void *, const void *);

#define MIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

#define ROUNDUP(A,B) (char *) (((uint64_t) A + B) & ~B);

#define MAXLINE 1024
#define ALIGNFILE 512         // same as in other classes
#define FILECHUNK 128
#define VALUECHUNK 128
#define MBYTES 100
#define ALIGNKV 4

enum{KVFILE,KMVFILE,SORTFILE};

/* ----------------------------------------------------------------------
   construct using caller's MPI communicator
   perform no MPI_init() and no MPI_Finalize()
------------------------------------------------------------------------- */

MapReduce::MapReduce(MPI_Comm caller)
{
  instances_now++;
  instances_ever++;
  instance_me = instances_ever;

  comm = caller;
  MPI_Comm_rank(comm,&me);
  MPI_Comm_size(comm,&nprocs);

  defaults();
}

/* ----------------------------------------------------------------------
   construct without MPI communicator, use MPI_COMM_WORLD
   perform MPI_Init() if not already initialized
   perform no MPI_Finalize()
------------------------------------------------------------------------- */

MapReduce::MapReduce()
{
  instances_now++;
  instances_ever++;
  instance_me = instances_ever;

  int flag;
  MPI_Initialized(&flag);

  if (!flag) {
    int argc = 0;
    char **argv = NULL;
    MPI_Init(&argc,&argv);
  }

  comm = MPI_COMM_WORLD;
  MPI_Comm_rank(comm,&me);
  MPI_Comm_size(comm,&nprocs);

  defaults();
}

/* ----------------------------------------------------------------------
   construct without MPI communicator, use MPI_COMM_WORLD
   perform MPI_Init() if not already initialized
   perform MPI_Finalize() if final instance is destructed
------------------------------------------------------------------------- */

MapReduce::MapReduce(double dummy)
{
  instances_now++;
  instances_ever++;
  instance_me = instances_ever;
  mpi_finalize_flag = 1;

  int flag;
  MPI_Initialized(&flag);

  if (!flag) {
    int argc = 0;
    char **argv = NULL;
    MPI_Init(&argc,&argv);
  }

  comm = MPI_COMM_WORLD;
  MPI_Comm_rank(comm,&me);
  MPI_Comm_size(comm,&nprocs);

  defaults();
}

/* ----------------------------------------------------------------------
   free all memory
   if finalize_flag is set and this is last instance, then finalize MPI
------------------------------------------------------------------------- */

MapReduce::~MapReduce()
{
  delete memory;
  delete error;
  delete [] fpath;
  delete kv;
  delete kmv;

  memory->sfree(memblock);

  instances_now--;
  if (instances_now == 0 && verbosity) cummulative_stats(verbosity,1);
  if (mpi_finalize_flag && instances_now == 0) MPI_Finalize();
}

/* ----------------------------------------------------------------------
   default settings
------------------------------------------------------------------------- */

void MapReduce::defaults()
{
  memory = new Memory(comm);
  error = new Error(comm);

  fpath = new char[2];
  strcpy(fpath,".");

  mapstyle = 0;
  verbosity = 0;
  timer = 0;
  memsize = MBYTES;
  keyalign = valuealign = ALIGNKV;

  twolenbytes = 2*sizeof(int);
  blockvalid = 0;

  allocated = 0;
  memblock = NULL;
  kv = NULL;
  kmv = NULL;

  if (sizeof(uint64_t) != 8 || sizeof(char *) != 8)
    error->all("Not compiled for 8-byte integers and pointers");

  int mpisize;
  MPI_Type_size(MPI_UNSIGNED_LONG,&mpisize);
  if (mpisize != 8)
    error->all("MPI_UNSIGNED_LONG is not 8-byte data type");
}

/* ----------------------------------------------------------------------
   make a copy of myself and return it
   new MR object duplicates my settings and KV/KMV
------------------------------------------------------------------------- */

MapReduce *MapReduce::copy()
{
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  MapReduce *mrnew = new MapReduce(comm);

  mrnew->mapstyle = mapstyle;
  mrnew->verbosity = verbosity;
  mrnew->timer = timer;
  mrnew->memsize = memsize;

  if (allocated) {
    mrnew->keyalign = kalign;
    mrnew->valuealign = valign;
  } else {
    mrnew->keyalign = keyalign;
    mrnew->valuealign = valuealign;
  }

  if (kv) mrnew->copy_kv(kv);
  if (kmv) mrnew->copy_kmv(kmv);

  if (kv) stats("Copy",0);
  if (kmv) stats("Copy",1);

  return mrnew;
}

/* ----------------------------------------------------------------------
   create my KV as copy of kv_src
   called by other MR's copy(), so my KV will not yet exist
------------------------------------------------------------------------- */

void MapReduce::copy_kv(KeyValue *kv_src)
{
  if (!allocated) allocate();
  char *fname = file_create(KVFILE,instance_me);
  kv = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
  memswap(fname);
  kv->copy(kv_src);
}

/* ----------------------------------------------------------------------
   create my KMV as copy of kmvsrc
   called by other MR's copy(), so my KMV will not yet exist
------------------------------------------------------------------------- */

void MapReduce::copy_kmv(KeyMultiValue *kmv_src)
{
  if (!allocated) allocate();
  char *fname = file_create(KMVFILE,instance_me);
  kmv = new KeyMultiValue(comm,memavail,memquarter,kalign,valign,fname);
  memswap(fname);
  kmv->copy(kmv_src);
}

/* ----------------------------------------------------------------------
   allocate big block of memory for KVs and KMVs
------------------------------------------------------------------------- */

void MapReduce::allocate()
{
  // allocate one big block of aligned memory

  if (memsize <= 0) error->all("Invalid memsize setting");
  memfull = ((size_t) memsize) * 1024*1024;
  memblock = (char *) memory->smalloc_align(memfull,ALIGNFILE,"MR:memblock");
  memset(memblock,0,memfull);

  // check key,value alignment factors

  kalign = keyalign;
  valign = valuealign;

  int tmp = 1;
  while (tmp < kalign) tmp *= 2;
  if (tmp != kalign) error->all("Invalid alignment setting");
  tmp = 1;
  while (tmp < valign) tmp *= 2;
  if (tmp != valign) error->all("Invalid alignment setting");

  talign = MAX(kalign,valign);
  talign = MAX(talign,sizeof(int));

  kalignm1 = kalign - 1;
  valignm1 = valign - 1;
  talignm1 = talign - 1;

  // mem0,mem1 = ptrs to first 2 quarters of memblock for KV & KMVs
  // mem2 = ptr to remaining half as scratch space
  // no need to align subsections because nbytes is Mbyte multiple

  memhalf = memfull/2;
  memquarter = memfull/4;
  mem0 = memblock;
  mem1 = &memblock[memquarter];
  mem2 = &memblock[memhalf];
  memavail = mem0;
  memtoggle = 0;
  allocated = 1;
}

/* ----------------------------------------------------------------------
   swap which of 2 sub-blocks is available
   also delete fname
------------------------------------------------------------------------- */

void MapReduce::memswap(char *fname)
{
  if (memavail == mem0) {
    memavail = mem1;
    memtoggle = 1;
  } else {
    memavail = mem0;
    memtoggle = 0;
  }
  delete [] fname;
}

/* ----------------------------------------------------------------------
   add KV pairs from another MR to my KV
------------------------------------------------------------------------- */

uint64_t MapReduce::add(MapReduce *mr)
{
  if (kv == NULL) error->all("Cannot add without KeyValue");
  if (mr->kv == NULL) 
    error->all("MapReduce passed to add() does not have KeyValue pairs");
  if (mr == this) error->all("Cannot add to self");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  if (!allocated) allocate();

  kv->append();
  kv->add(mr->kv);
  kv->complete();

  stats("Add",0);

  uint64_t nkeyall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   aggregate a KV across procs to create a new KV
   initially, key copies can exist on many procs
   after aggregation, all copies of key are on same proc
   performed via parallel distributed hashing
   hash = user hash function (NULL if not provided)
   requires irregular all2all communication
------------------------------------------------------------------------- */

uint64_t MapReduce::aggregate(int (*hash)(char *, int))
{
  int i,nbytes,dummy1,dummy2,dummy3;
  int nkey_send,nkey_recv;
  int keybytes,valuebytes,keybytes_align,valuebytes_align;
  int maxsend,maxrecv,maxbytes;
  char *ptr,*ptr_start,*key;

  if (kv == NULL) error->all("Cannot aggregate without KeyValue");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  if (nprocs == 1) {
    stats("Aggregate",0);
    return kv->nkv;
  }

  char *fname = file_create(KVFILE,instance_me);
  KeyValue *kvnew = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
  memswap(fname);

  Irregular *irregular = new Irregular(comm);

  int *proclist = NULL;
  int *sendsizes = NULL;
  int *recvsizes = NULL;
  char *bufkv = NULL;
  maxsend = maxrecv = maxbytes = 0;

  // maxpage = max # of pages in any proc's KV

  char *page_send,*page_recv;
  int npage_send = kv->request_info(&page_send);
  int maxpage;
  MPI_Allreduce(&npage_send,&maxpage,1,MPI_INT,MPI_MAX,comm);

  // loop over pages, perform irregular comm on each

  for (int ipage = 0; ipage < maxpage; ipage++) {

    // load page of KV pairs

    if (ipage < npage_send)
      nkey_send = kv->request_page(ipage,dummy1,dummy2,dummy3);
    else nkey_send = 0;

    // allocate send lists

    if (maxsend < nkey_send) {
      memory->sfree(proclist);
      memory->sfree(sendsizes);
      maxsend = nkey_send;
      proclist = (int *) memory->smalloc(maxsend*sizeof(int),"MR:proclist");
      sendsizes = (int *) memory->smalloc(maxsend*sizeof(int),"MR:sendsizes");
    }

    // hash each key to a proc ID
    // via either user-provided hash function or hashlittle()

    ptr = page_send;

    for (i = 0; i < nkey_send; i++) {
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

      sendsizes[i] = ptr - ptr_start;
      if (hash) proclist[i] = hash(key,keybytes) % nprocs;
      else proclist[i] = hashlittle(key,keybytes,nprocs) % nprocs;
    }

    // redistribute KV pairs
    // same irregular pattern works for recvsizes and KV data
    // insure recvsizes and arrays are big enough for incoming data
    // add received KV pairs to kvnew

    irregular->pattern(nkey_send,proclist);

    nkey_recv = irregular->size(sizeof(int)) / sizeof(int);
    if (nkey_recv > maxrecv) {
      memory->sfree(recvsizes);
      maxrecv = nkey_recv;
      recvsizes = (int *) memory->smalloc(maxrecv*sizeof(int),"MR:recvsizes");
    }
    irregular->exchange((char *) sendsizes,(char *) recvsizes);
    cssize += irregular->cssize;
    crsize += irregular->crsize;

    // use mem2 for received KVs if large enough (2x more than page)
    // else use bufkv

    nbytes = irregular->size(sendsizes,NULL,recvsizes);
    if (nbytes <= memhalf) page_recv = mem2;
    else if (nbytes <= maxbytes) page_recv = bufkv;
    else {
      memory->sfree(bufkv);
      maxbytes = nbytes;
      bufkv = (char *) memory->smalloc(maxbytes,"MR:bufkv");
      page_recv = bufkv;
    }
    irregular->exchange(page_send,page_recv);
    cssize += irregular->cssize;
    crsize += irregular->crsize;

    // add received KV pairs to kvnew

    kvnew->add(nkey_recv,page_recv);
  }

  memory->sfree(proclist);
  memory->sfree(sendsizes);
  memory->sfree(recvsizes);
  memory->sfree(bufkv);
  delete irregular;

  delete kv;
  kv = kvnew;
  kv->complete();

  stats("Aggregate",0);

  uint64_t nkeyall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   clone KV to KMV so that KMV pairs are one-to-one copies of KV pairs
   each proc clones only its data
   assume each KV key is unique, but is not required
------------------------------------------------------------------------- */

uint64_t MapReduce::clone()
{
  if (kv == NULL) error->all("Cannot clone without KeyValue");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  char *fname = file_create(KMVFILE,instance_me);
  kmv = new KeyMultiValue(comm,memavail,memquarter,kalign,valign,fname);
  memswap(fname);

  kmv->clone(kv);
  kmv->complete();

  delete kv;
  kv = NULL;

  stats("Clone",1);

  uint64_t nkeyall;
  MPI_Allreduce(&kmv->nkmv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   collapse KV into a KMV with a single key/value
   each proc collapses only its data
   new key = provided key name (same on every proc)
   new value = list of old key,value,key,value,etc
------------------------------------------------------------------------- */

uint64_t MapReduce::collapse(char *key, int keybytes)
{
  if (kv == NULL) error->all("Cannot collapse without KeyValue");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  char *fname = file_create(KMVFILE,instance_me);
  kmv = new KeyMultiValue(comm,memavail,memquarter,kalign,valign,fname);
  memswap(fname);

  kmv->collapse(key,keybytes,kv);
  kmv->complete();

  delete kv;
  kv = NULL;

  stats("Collapse",1);

  uint64_t nkeyall;
  MPI_Allreduce(&kmv->nkmv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   collate KV to create a KMV
   aggregate followed by a convert
   hash = user hash function (NULL if not provided)
------------------------------------------------------------------------- */

uint64_t MapReduce::collate(int (*hash)(char *, int))
{
  if (kv == NULL) error->all("Cannot collate without KeyValue");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  int verbosity_hold = verbosity;
  int timer_hold = timer;
  verbosity = timer = 0;

  aggregate(hash);
  convert();

  verbosity = verbosity_hold;
  timer = timer_hold;
  stats("Collate",1);

  uint64_t nkeyall;
  MPI_Allreduce(&kmv->nkmv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   compress KV to create a smaller KV
   duplicate keys are replaced with a single key/value
   each proc compresses only its data
   create a KMV temporarily
   call appcompress() with each key/multivalue in KMV
   appcompress() returns single key/value to new KV
------------------------------------------------------------------------- */

uint64_t MapReduce::compress(void (*appcompress)(char *, int, char *,
						 int, int *, KeyValue *, 
						 void *),
			     void *appptr)
{
  int dummy1,dummy2,dummy3;
  char *keycopy;

  if (kv == NULL) error->all("Cannot compress without KeyValue");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  char *fname = file_create(KMVFILE,instance_me);
  kmv = new KeyMultiValue(comm,memavail,memquarter,kalign,valign,fname);
  memswap(fname);

  kmv->convert(kv,mem2,memhalf,fpath);
  kmv->complete();

  delete kv;
  fname = file_create(KVFILE,instance_me);
  kv = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
  memswap(fname);

  int nkey,keybytes,mvaluebytes,nvalues;
  int *valuesizes;
  char *ptr,*key,*multivalue;

  char *page;
  int npage = kmv->request_info(&page);

  for (int ipage = 0; ipage < npage; ipage++) {
    nkey = kmv->request_page(ipage,0,dummy1,dummy2,dummy3);

    ptr = page;

    for (int i = 0; i < nkey; i++) {
      keybytes = *((int *) ptr);
      ptr += sizeof(int);
      mvaluebytes = *((int *) ptr);
      ptr += sizeof(int);
      nvalues = *((int *) ptr);
      ptr += sizeof(int);

      if (nvalues > 0) {
	valuesizes = (int *) ptr;
	ptr += nvalues*sizeof(int);

	ptr = ROUNDUP(ptr,kalignm1);
	key = ptr;
	ptr += keybytes;
	ptr = ROUNDUP(ptr,valignm1);
	multivalue = ptr;
	ptr += mvaluebytes;
	ptr = ROUNDUP(ptr,talignm1);
	
	appcompress(key,keybytes,multivalue,nvalues,valuesizes,kv,appptr);

      } else {
	nblock_kmv = -nvalues;
	
	ptr = ROUNDUP(ptr,kalignm1);
	key = ptr;

	// make copy of key because subsequent calls to
	// multivalue_block() will overwrite page

	keycopy = new char[keybytes];
	memcpy(keycopy,key,keybytes);

	block_header_page = ipage;
	blockvalid = 1;
	appcompress(keycopy,keybytes,NULL,nvalues,(int *) this,kv,appptr);
	blockvalid = 0;
	ipage += nblock_kmv;

	delete [] keycopy;
      }
    }
  }

  // insure KMV file is closed, if last request_page() was for extended page

  kmv->close_file();

  kv->complete();

  delete kmv;
  kmv = NULL;

  stats("Compress",0);

  uint64_t nkeyall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   convert KV to KMV
   duplicate keys are replaced with a single key/multivalue
   each proc converts only its data
   new key = old unique key
   new multivalue = concatenated list of all values for that key in KV
------------------------------------------------------------------------- */

uint64_t MapReduce::convert()
{
  if (kv == NULL) error->all("Cannot convert without KeyValue");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  char *fname = file_create(KMVFILE,instance_me);
  kmv = new KeyMultiValue(comm,memavail,memquarter,kalign,valign,fname);
  memswap(fname);

  kmv->convert(kv,mem2,memhalf,fpath);
  kmv->complete();

  delete kv;
  kv = NULL;

  stats("Convert",1);

  uint64_t nkeyall;
  MPI_Allreduce(&kmv->nkmv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   gather a distributed KV to a new KV on fewer procs
   numprocs = # of procs new KV resides on (0 to numprocs-1)
------------------------------------------------------------------------- */

uint64_t MapReduce::gather(int numprocs)
{
  int i,flag,npage,nkey;
  char *buf;
  int sizes[4];
  MPI_Status status;
  MPI_Request request;

  if (kv == NULL) error->all("Cannot gather without KeyValue");
  if (numprocs < 1 || numprocs > nprocs) 
    error->all("Invalid proc count for gather");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  if (nprocs == 1 || numprocs == nprocs) {
    stats("Gather",0);
    uint64_t nkeyall;
    MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
    return nkeyall;
  }

  // lo procs collect key/value pairs from hi procs
  // lo procs are those with ID < numprocs
  // lo procs recv from set of hi procs with same (ID % numprocs)

  if (me < numprocs) {
    kv->append();
    buf = memavail;

    for (int iproc = me+numprocs; iproc < nprocs; iproc += numprocs) {
      MPI_Send(&flag,0,MPI_INT,iproc,0,comm);
      MPI_Recv(&npage,1,MPI_INT,iproc,0,comm,&status);
      
      for (int ipage = 0; ipage < npage; ipage++) {
	MPI_Irecv(buf,memquarter,MPI_BYTE,iproc,1,comm,&request);
	MPI_Send(&flag,0,MPI_INT,iproc,0,comm);
	MPI_Recv(sizes,4,MPI_INT,iproc,0,comm,&status);
	crsize += sizes[3];
	MPI_Wait(&request,&status);
	kv->add(sizes[0],buf,sizes[1],sizes[2],sizes[3]);
      }
    }

  } else {
    int iproc = me % numprocs;
    npage = kv->request_info(&buf);

    MPI_Recv(&flag,0,MPI_INT,iproc,0,comm,&status);
    MPI_Send(&npage,1,MPI_INT,iproc,0,comm);

    for (int ipage = 0; ipage < npage; ipage++) {
      sizes[0] = kv->request_page(ipage,sizes[1],sizes[2],sizes[3]);
      MPI_Recv(&flag,0,MPI_INT,iproc,0,comm,&status);
      MPI_Send(sizes,4,MPI_INT,iproc,0,comm);
      MPI_Send(buf,sizes[3],MPI_BYTE,iproc,1,comm);
      cssize += sizes[3];
    }

    delete kv;
    char *fname = file_create(KVFILE,instance_me);
    kv = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
    memswap(fname);
  }

  kv->complete();

  stats("Gather",0);

  uint64_t nkeyall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   create a KV via a parallel map operation for nmap tasks
   make one call to appmap() for each task
   mapstyle determines how tasks are partitioned to processors
------------------------------------------------------------------------- */

uint64_t MapReduce::map(int nmap, void (*appmap)(int, KeyValue *, void *),
			void *appptr, int addflag)
{
  MPI_Status status;

  if (timer) start_timer();
  if (verbosity) file_stats(0);

  if (!allocated) allocate();
  delete kmv;
  kmv = NULL;

  if (addflag == 0) {
    delete kv;
    char *fname = file_create(KVFILE,instance_me);
    kv = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
    memswap(fname);
  } else if (kv == NULL) {
    char *fname = file_create(KVFILE,instance_me);
    kv = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
    memswap(fname);
  } else {
    kv->append();
  }

  // nprocs = 1 = all tasks to single processor
  // mapstyle 0 = chunk of tasks to each proc
  // mapstyle 1 = strided tasks to each proc
  // mapstyle 2 = master/slave assignment of tasks

  if (nprocs == 1) {
    for (int itask = 0; itask < nmap; itask++)
      appmap(itask,kv,appptr);

  } else if (mapstyle == 0) {
    uint64_t nmap64 = nmap;
    int lo = me * nmap64 / nprocs;
    int hi = (me+1) * nmap64 / nprocs;
    for (int itask = lo; itask < hi; itask++)
      appmap(itask,kv,appptr);

  } else if (mapstyle == 1) {
    for (int itask = me; itask < nmap; itask += nprocs)
      appmap(itask,kv,appptr);

  } else if (mapstyle == 2) {
    if (me == 0) {
      int doneflag = -1;
      int ndone = 0;
      int itask = 0;
      for (int iproc = 1; iproc < nprocs; iproc++) {
	if (itask < nmap) {
	  MPI_Send(&itask,1,MPI_INT,iproc,0,comm);
	  itask++;
	} else {
	  MPI_Send(&doneflag,1,MPI_INT,iproc,0,comm);
	  ndone++;
	}
      }
      while (ndone < nprocs-1) {
	int iproc,tmp;
	MPI_Recv(&tmp,1,MPI_INT,MPI_ANY_SOURCE,0,comm,&status);
	iproc = status.MPI_SOURCE;

	if (itask < nmap) {
	  MPI_Send(&itask,1,MPI_INT,iproc,0,comm);
	  itask++;
	} else {
	  MPI_Send(&doneflag,1,MPI_INT,iproc,0,comm);
	  ndone++;
	}
      }

    } else {
      while (1) {
	int itask;
	MPI_Recv(&itask,1,MPI_INT,0,0,comm,&status);
	if (itask < 0) break;
	appmap(itask,kv,appptr);
	MPI_Send(&itask,1,MPI_INT,0,0,comm);
      }
    }

  } else error->all("Invalid mapstyle setting");

  kv->complete();

  stats("Map",0);

  uint64_t nkeyall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   create a KV via a parallel map operation for list of files in file
   make one call to appmap() for each file in file
   mapstyle determines how tasks are partitioned to processors
------------------------------------------------------------------------- */

uint64_t MapReduce::map(char *file, 
			void (*appmap)(int, char *, KeyValue *, void *),
			void *appptr, int addflag)
{
  int n;
  char line[MAXLINE];
  MPI_Status status;

  if (timer) start_timer();
  if (verbosity) file_stats(0);

  if (!allocated) allocate();
  delete kmv;
  kmv = NULL;

  if (addflag == 0) {
    delete kv;
    char *fname = file_create(KVFILE,instance_me);
    kv = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
    memswap(fname);
  } else if (kv == NULL) {
    char *fname = file_create(KVFILE,instance_me);
    kv = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
    memswap(fname);
  } else {
    kv->append();
  }

  // open file and extract filenames
  // bcast each filename to all procs
  // trim whitespace from beginning and end of filename

  int nmap = 0;
  int maxfiles = 0;
  char **files = NULL;
  FILE *fp;

  if (me == 0) {
    fp = fopen(file,"r");
    if (fp == NULL) error->one("Could not open file of file names");
  }

  while (1) {
    if (me == 0) {
      if (fgets(line,MAXLINE,fp) == NULL) n = 0;
      else n = strlen(line) + 1;
    }
    MPI_Bcast(&n,1,MPI_INT,0,comm);
    if (n == 0) {
      if (me == 0) fclose(fp);
      break;
    }

    MPI_Bcast(line,n,MPI_CHAR,0,comm);

    char *ptr = line;
    while (isspace(*ptr)) ptr++;
    if (strlen(ptr) == 0) error->all("Blank line in file of file names");
    char *ptr2 = ptr + strlen(ptr) - 1;
    while (isspace(*ptr2)) ptr2--;
    ptr2++;
    *ptr2 = '\0';

    if (nmap == maxfiles) {
      maxfiles += FILECHUNK;
      files = (char **)
	memory->srealloc(files,maxfiles*sizeof(char *),"MR:files");
    }
    n = strlen(ptr) + 1;
    files[nmap] = new char[n];
    strcpy(files[nmap],ptr);
    nmap++;
  }
  
  // nprocs = 1 = all tasks to single processor
  // mapstyle 0 = chunk of tasks to each proc
  // mapstyle 1 = strided tasks to each proc
  // mapstyle 2 = master/slave assignment of tasks

  if (nprocs == 1) {
    for (int itask = 0; itask < nmap; itask++)
      appmap(itask,files[itask],kv,appptr);

  } else if (mapstyle == 0) {
    uint64_t nmap64 = nmap;
    int lo = me * nmap64 / nprocs;
    int hi = (me+1) * nmap64 / nprocs;
    for (int itask = lo; itask < hi; itask++)
      appmap(itask,files[itask],kv,appptr);

  } else if (mapstyle == 1) {
    for (int itask = me; itask < nmap; itask += nprocs)
      appmap(itask,files[itask],kv,appptr);

  } else if (mapstyle == 2) {
    if (me == 0) {
      int doneflag = -1;
      int ndone = 0;
      int itask = 0;
      for (int iproc = 1; iproc < nprocs; iproc++) {
	if (itask < nmap) {
	  MPI_Send(&itask,1,MPI_INT,iproc,0,comm);
	  itask++;
	} else {
	  MPI_Send(&doneflag,1,MPI_INT,iproc,0,comm);
	  ndone++;
	}
      }
      while (ndone < nprocs-1) {
	int iproc,tmp;
	MPI_Recv(&tmp,1,MPI_INT,MPI_ANY_SOURCE,0,comm,&status);
	iproc = status.MPI_SOURCE;

	if (itask < nmap) {
	  MPI_Send(&itask,1,MPI_INT,iproc,0,comm);
	  itask++;
	} else {
	  MPI_Send(&doneflag,1,MPI_INT,iproc,0,comm);
	  ndone++;
	}
      }

    } else {
      while (1) {
	int itask;
	MPI_Recv(&itask,1,MPI_INT,0,0,comm,&status);
	if (itask < 0) break;
	appmap(itask,files[itask],kv,appptr);
	MPI_Send(&itask,1,MPI_INT,0,0,comm);
      }
    }

  } else error->all("Invalid mapstyle setting");

  // clean up file list

  for (int i = 0; i < nmap; i++) delete files[i];
  memory->sfree(files);

  kv->complete();

  stats("Map",0);

  uint64_t nkeyall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   create a KV via a parallel map operation for nmap tasks
   nfiles filenames are split into nmap pieces based on separator char
------------------------------------------------------------------------- */

uint64_t MapReduce::map(int nmap, int nfiles, char **files,
			char sepchar, int delta,
			void (*appmap)(int, char *, int, KeyValue *, void *),
			void *appptr, int addflag)
{
  filemap.sepwhich = 1;
  filemap.sepchar = sepchar;
  filemap.delta = delta;

  return map_file(nmap,nfiles,files,appmap,appptr,addflag);
}

/* ----------------------------------------------------------------------
   create a KV via a parallel map operation for nmap tasks
   nfiles filenames are split into nmap pieces based on separator string
------------------------------------------------------------------------- */

uint64_t MapReduce::map(int nmap, int nfiles, char **files,
			char *sepstr, int delta,
			void (*appmap)(int, char *, int, KeyValue *, void *),
			void *appptr, int addflag)
{
  filemap.sepwhich = 0;
  int n = strlen(sepstr) + 1;
  filemap.sepstr = new char[n];
  strcpy(filemap.sepstr,sepstr);
  filemap.delta = delta;

  return map_file(nmap,nfiles,files,appmap,appptr,addflag);
}

/* ----------------------------------------------------------------------
   called by 2 map methods that take files and a separator
   create a KV via a parallel map operation for nmap tasks
   nfiles filenames are split into nmap pieces based on separator
   FileMap struct stores info on how to split files
   calls non-file map() to partition tasks to processors
     with callback to non-class map_file_standalone()
   map_file_standalone() reads chunk of file and passes it to user appmap()
------------------------------------------------------------------------- */

uint64_t MapReduce::map_file(int nmap, int nfiles, char **files,
			     void (*appmap)(int, char *, int, KeyValue *, void *),
			     void *appptr, int addflag)
{
  if (nfiles > nmap) error->all("Cannot map with more files than tasks");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  if (!allocated) allocate();
  delete kmv;
  kmv = NULL;

  // copy filenames into FileMap

  filemap.filename = new char*[nfiles];
  for (int i = 0; i < nfiles; i++) {
    int n = strlen(files[i]) + 1;
    filemap.filename[i] = new char[n];
    strcpy(filemap.filename[i],files[i]);
  }

  // get filesize of each file via stat()
  // proc 0 queries files, bcasts results to all procs

  filemap.filesize = new uint64_t[nfiles];
  struct stat stbuf;

  if (me == 0) {
    for (int i = 0; i < nfiles; i++) {
      int flag = stat(files[i],&stbuf);
      if (flag < 0) error->one("Could not query file size");
      filemap.filesize[i] = stbuf.st_size;
    }
  }

  MPI_Bcast(filemap.filesize,nfiles*sizeof(uint64_t),MPI_BYTE,0,comm);

  // ntotal = total size of all files
  // nideal = ideal # of bytes per task

  uint64_t ntotal = 0;
  for (int i = 0; i < nfiles; i++) ntotal += filemap.filesize[i];
  uint64_t nideal = MAX(1,ntotal/nmap);

  // tasksperfile[i] = # of tasks for Ith file
  // initial assignment based on ideal chunk size
  // increment/decrement tasksperfile until reach target # of tasks
  // even small files must have 1 task

  filemap.tasksperfile = new int[nfiles];

  int ntasks = 0;
  for (int i = 0; i < nfiles; i++) {
    filemap.tasksperfile[i] = MAX(1,filemap.filesize[i]/nideal);
    ntasks += filemap.tasksperfile[i];
  }

  while (ntasks < nmap)
    for (int i = 0; i < nfiles; i++)
      if (filemap.filesize[i] > nideal) {
	filemap.tasksperfile[i]++;
	ntasks++;
	if (ntasks == nmap) break;
      }
  while (ntasks > nmap)
    for (int i = 0; i < nfiles; i++)
      if (filemap.tasksperfile[i] > 1) {
	filemap.tasksperfile[i]--;
	ntasks--;
	if (ntasks == nmap) break;
      }

  // check if any tasks are so small they will cause overlapping reads w/ delta
  // if so, reduce number of tasks for that file and issue warning

  int flag = 0;
  for (int i = 0; i < nfiles; i++) {
    if (filemap.filesize[i] / filemap.tasksperfile[i] > filemap.delta)
      continue;
    flag = 1;
    while (filemap.tasksperfile[i] > 1) {
      filemap.tasksperfile[i]--;
      nmap--;
      if (filemap.filesize[i] / filemap.tasksperfile[i] > filemap.delta) break;
    }
  }

  if (flag & me == 0) {
    char str[128];
    sprintf(str,"File(s) too small for file delta - decreased map tasks to %d",
	    nmap);
    error->warning(str);
  }

  // whichfile[i] = which file is associated with the Ith task
  // whichtask[i] = which task in that file the Ith task is

  filemap.whichfile = new int[nmap];
  filemap.whichtask = new int[nmap];

  int itask = 0;
  for (int i = 0; i < nfiles; i++)
    for (int j = 0; j < filemap.tasksperfile[i]; j++) {
      filemap.whichfile[itask] = i;
      filemap.whichtask[itask++] = j;
    }

  // use non-file map() to partition tasks to procs
  // it calls map_file_standalone once for each task

  int verbosity_hold = verbosity;
  int timer_hold = timer;
  verbosity = timer = 0;

  filemap.appmapfile = appmap;
  filemap.appptr = appptr;
  map(nmap,&map_file_standalone,this,addflag);

  verbosity = verbosity_hold;
  timer = timer_hold;
  stats("Map",0);

  // destroy FileMap

  if (filemap.sepwhich == 0) delete [] filemap.sepstr;
  for (int i = 0; i < nfiles; i++) delete [] filemap.filename[i];
  delete [] filemap.filename;
  delete [] filemap.filesize;
  delete [] filemap.tasksperfile;
  delete [] filemap.whichfile;
  delete [] filemap.whichtask;

  uint64_t nkeyall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   wrappers on user-provided appmapfile function
   2-level wrapper needed b/c file map() calls non-file map()
     and cannot pass it a class method unless it were static,
     but then it couldn't access MR class data
   so non-file map() is passed standalone non-class method
   standalone calls back into class wrapper which calls user appmapfile()
------------------------------------------------------------------------- */

void map_file_standalone(int imap, KeyValue *kv, void *ptr)
{
  MapReduce *mr = (MapReduce *) ptr;
  mr->map_file_wrapper(imap,kv);
}

void MapReduce::map_file_wrapper(int imap, KeyValue *kv)
{
  // readstart = position in file to start reading for this task
  // readsize = # of bytes to read including delta

  uint64_t filesize = filemap.filesize[filemap.whichfile[imap]];
  int itask = filemap.whichtask[imap];
  int ntask = filemap.tasksperfile[filemap.whichfile[imap]];

  uint64_t readstart = itask*filesize/ntask;
  uint64_t readnext = (itask+1)*filesize/ntask;
  int readsize = readnext - readstart + filemap.delta;
  readsize = MIN(readsize,filesize-readstart);

  // read from appropriate file
  // terminate string with NULL

  char *str = new char[readsize+1];
  FILE *fp = fopen(filemap.filename[filemap.whichfile[imap]],"rb");
  fseek(fp,readstart,SEEK_SET);
  fread(str,1,readsize,fp);
  str[readsize] = '\0';
  fclose(fp);

  // if not first task in file, trim start of string
  // separator can be single char or a string
  // str[strstart] = 1st char in string
  // if separator = char, strstart is char after separator
  // if separator = string, strstart is 1st char of separator

  int strstart = 0;
  if (itask > 0) {
    char *ptr;
    if (filemap.sepwhich) ptr = strchr(str,filemap.sepchar);
    else ptr = strstr(str,filemap.sepstr);
    if (ptr == NULL || ptr-str > filemap.delta)
      error->one("Could not find separator within delta");
    strstart = ptr-str + filemap.sepwhich;
  }

  // if not last task in file, trim end of string
  // separator can be single char or a string
  // str[strstop] = last char in string = inserted NULL
  // if separator = char, NULL is char after separator
  // if separator = string, NULL is 1st char of separator

  int strstop = readsize;
  if (itask < ntask-1) {
    char *ptr;
    if (filemap.sepwhich) 
      ptr = strchr(&str[readnext-readstart],filemap.sepchar);
    else 
      ptr = strstr(&str[readnext-readstart],filemap.sepstr);
    if (ptr == NULL) error->one("Could not find separator within delta");
    if (filemap.sepwhich) ptr++;
    *ptr = '\0';
    strstop = ptr-str;
  }

  // call user appmapfile() function with user data ptr

  int strsize = strstop - strstart + 1;
  filemap.appmapfile(imap,&str[strstart],strsize,kv,filemap.appptr);
  delete [] str;
}

/* ----------------------------------------------------------------------
   create a KV via a parallel map operation from an existing MR's KV
   make one call to appmap() for each key/value pair in the input MR's KV
   each proc operates on key/value pairs it owns
------------------------------------------------------------------------- */

uint64_t MapReduce::map(MapReduce *mr, 
			void (*appmap)(int, char *, int, char *, int, 
				       KeyValue *, void *),
			void *appptr, int addflag)
{
  if (mr->kv == NULL)
    error->all("MapReduce passed to map() does not have KeyValue pairs");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  if (!allocated) allocate();
  delete kmv;
  kmv = NULL;

  // kv_src = KeyValue object which sends KV pairs to appmap()
  // kv_dest = KeyValue object which stores new KV pairs
  // if mr = this and addflag, then 2 KVs are the same, copy KV first

  KeyValue *kv_src = mr->kv;
  KeyValue *kv_dest;

  if (mr == this) {
    if (addflag) {
      char *fname = file_create(KVFILE,instance_me);
      kv_dest = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
      memswap(fname);
      kv_dest->copy(kv_src);
      kv_dest->append();
    } else {
      char *fname = file_create(KVFILE,instance_me);
      kv_dest = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
      memswap(fname);
    }
  } else {
    if (addflag == 0) {
      delete kv;
      char *fname = file_create(KVFILE,instance_me);
      kv_dest = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
      memswap(fname);
    } else if (kv == NULL) {
      char *fname = file_create(KVFILE,instance_me);
      kv_dest = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
      memswap(fname);
    } else {
      kv->append();
      kv_dest = kv;
    }
  }

  int nkey,dummy1,dummy2,dummy3;
  int keybytes,valuebytes;
  char *page,*ptr,*key,*value;
  int npage = kv_src->request_info(&page);

  for (int ipage = 0; ipage < npage; ipage++) {
    nkey = kv_src->request_page(ipage,dummy1,dummy2,dummy3);
    
    ptr = page;

    for (int i = 0; i < nkey; i++) {
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
      
      appmap(i,key,keybytes,value,valuebytes,kv_dest,appptr);
    }
  }

  if (mr == this) delete kv_src;
  kv = kv_dest;
  kv->complete();

  stats("Map",0);

  uint64_t nkeyall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   create a KV from a KMV via a parallel reduce operation for nmap tasks
   make one call to appreduce() for each KMV pair
   each proc processes its owned KMV pairs
------------------------------------------------------------------------- */

uint64_t MapReduce::reduce(void (*appreduce)(char *, int, char *,
					     int, int *, KeyValue *, void *),
			   void *appptr)
{
  int dummy1,dummy2,dummy3;
  char *keycopy;

  if (kmv == NULL) error->all("Cannot reduce without KeyMultiValue");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  char *fname = file_create(KVFILE,instance_me);
  kv = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
  memswap(fname);

  int nkey,keybytes,mvaluebytes,nvalues;
  int *valuesizes;
  char *ptr,*key,*multivalue;

  char *page;
  int npage = kmv->request_info(&page);

  for (int ipage = 0; ipage < npage; ipage++) {
    nkey = kmv->request_page(ipage,0,dummy1,dummy2,dummy3);

    ptr = page;

    for (int i = 0; i < nkey; i++) {
      keybytes = *((int *) ptr);
      ptr += sizeof(int);
      mvaluebytes = *((int *) ptr);
      ptr += sizeof(int);
      nvalues = *((int *) ptr);
      ptr += sizeof(int);

      if (nvalues > 0) {
	valuesizes = (int *) ptr;
	ptr += nvalues*sizeof(int);
	
	ptr = ROUNDUP(ptr,kalignm1);
	key = ptr;
	ptr += keybytes;
	ptr = ROUNDUP(ptr,valignm1);
	multivalue = ptr;
	ptr += mvaluebytes;
	ptr = ROUNDUP(ptr,talignm1);
	
	appreduce(key,keybytes,multivalue,nvalues,valuesizes,kv,appptr);

      } else {
	nvalues = *((int *) ptr);
	ptr += sizeof(int);
	nblock_kmv = *((int *) ptr);
	ptr += sizeof(int);
	
	ptr = ROUNDUP(ptr,kalignm1);
	key = ptr;

	// make copy of key because subsequent calls to
	// multivalue_block() will overwrite page

	keycopy = new char[keybytes];
	memcpy(keycopy,key,keybytes);

	block_header_page = ipage;
	blockvalid = 1;
	appreduce(keycopy,keybytes,NULL,nvalues,(int *) this,kv,appptr);
	blockvalid = 0;
	ipage += nblock_kmv;

	delete [] keycopy;
      }
    }
  }

  // insure KMV file is closed, if last request_page() was for extended page

  kmv->close_file();
  kv->complete();

  delete kmv;
  kmv = NULL;

  stats("Reduce",0);

  uint64_t nkeyall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   scrunch KV to create a KMV on fewer processors, each with a single pair
   gather followed by a collapse
   numprocs = # of procs new KMV resides on (0 to numprocs-1)
   new key = provided key name (same on every proc)
   new value = list of old key,value,key,value,etc
------------------------------------------------------------------------- */

uint64_t MapReduce::scrunch(int numprocs, char *key, int keybytes)
{
  if (kv == NULL) error->all("Cannot scrunch without KeyValue");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  int verbosity_hold = verbosity;
  int timer_hold = timer;
  verbosity = timer = 0;

  gather(numprocs);
  collapse(key,keybytes);

  verbosity = verbosity_hold;
  timer = timer_hold;
  stats("Scrunch",1);

  uint64_t nkeyall;
  MPI_Allreduce(&kmv->nkmv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   query # of blocks in a single KMV that spans multiple pages
   called from user myreduce() or mycompress() function
------------------------------------------------------------------------- */

int MapReduce::multivalue_blocks()
{
  if (!blockvalid) error->one("Invalid call to multivalue_blocks()");
  return nblock_kmv;
}

/* ----------------------------------------------------------------------
   query info for 1 block of a single KMV that spans multiple pages
   called from user myreduce() or mycompress() function
------------------------------------------------------------------------- */

int MapReduce::multivalue_block(int iblock, 
				char **pmultivalue, int **pvaluesizes)
{
  int dummy1,dummy2,dummy3;

  if (!blockvalid) error->one("Invalid call to multivalue_block()");
  if (iblock < 0 || iblock >= nblock_kmv)
    error->one("Invalid call to multivalue_block()");

  char *page;
  kmv->request_info(&page);
  kmv->request_page(block_header_page+iblock+1,0,dummy1,dummy2,dummy3);

  int nvalue = *((int *) page);
  *pvaluesizes = (int *) &page[sizeof(int)];

  char *ptr = &page[(nvalue+1)*sizeof(int)];
  ptr = ROUNDUP(ptr,valignm1);
  *pmultivalue = ptr;

  return nvalue;
}

/* ----------------------------------------------------------------------
   sort keys in a KV to create a new KV
   use appcompare() to compare 2 keys
   each proc sorts only its data
------------------------------------------------------------------------- */

uint64_t MapReduce::sort_keys(int (*appcompare)(char *, int, char *, int))
{
  if (kv == NULL) error->all("Cannot sort_keys without KeyValue");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  compare = appcompare;
  sort_kv(0);

  stats("Sort_keys",0);

  uint64_t nkeyall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   sort values in a KV to create a new KV
   use appcompare() to compare 2 values
   each proc sorts only its data
------------------------------------------------------------------------- */

uint64_t MapReduce::sort_values(int (*appcompare)(char *, int, char *, int))
{
  if (kv == NULL) error->all("Cannot sort_values without KeyValue");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  compare = appcompare;
  sort_kv(1);

  stats("Sort_values",0);

  uint64_t nkeyall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   sort values within each multivalue in a KMV
   sorts in place, does not create a new KMV
   use appcompare() to compare 2 values within a multivalue
   each proc sorts only its data
------------------------------------------------------------------------- */

uint64_t MapReduce::sort_multivalues(int (*appcompare)(char *, int, 
						       char *, int))
{
  int i,j,k;
  int dummy1,dummy2,dummy3;

  if (kmv == NULL) error->all("Cannot sort_multivalues without KeyMultiValue");
  if (timer) start_timer();
  if (verbosity) file_stats(0);

  char *page;
  int npage = kmv->request_info(&page);

  int maxn = 0;
  int *order = NULL;
  soffset = NULL;

  compare = appcompare;
  mrptr = this;

  int nkey,keybytes,mvaluebytes,nvalues;
  char *ptr,*multivalue;
  int *valuesizes;

  for (int ipage = 0; ipage < npage; ipage++) {
    nkey = kmv->request_page(ipage,1,dummy1,dummy2,dummy3);

    ptr = page;

    for (int i = 0; i < nkey; i++) {
      keybytes = *((int *) ptr);
      ptr += sizeof(int);
      mvaluebytes = *((int *) ptr);
      ptr += sizeof(int);
      nvalues = *((int *) ptr);
      ptr += sizeof(int);

      if (nvalues == 0)
	error->one("Cannot yet sort multivalues for a "
		   "multiple block KeyMultiValue");

      valuesizes = (int *) ptr;
      ptr += nvalues*sizeof(int);

      ptr = ROUNDUP(ptr,kalignm1);
      ptr += keybytes;
      ptr = ROUNDUP(ptr,valignm1);
      multivalue = ptr;
      ptr += mvaluebytes;
      ptr = ROUNDUP(ptr,talignm1);

      if (nvalues > maxn) {
	memory->sfree(order);
	memory->sfree(soffset);
	maxn = roundup(nvalues,VALUECHUNK);
	order = (int *) memory->smalloc(maxn*sizeof(int),"MR:order");
	soffset = (int *) memory->smalloc(maxn*sizeof(int),"MR:soffset");
      }

      // soffset = byte offset for each value within multivalue

      soffset[0] = 0;
      for (j = 1; j < nvalues; j++)
	soffset[j] = soffset[j-1] + valuesizes[j-1];

      // sort values within multivalue via qsort()
      
      sptr = multivalue;
      slength = valuesizes;
      qsort(order,nvalues,sizeof(int),compare_standalone);
      
      // reorder the multivalue, using memavail as scratch space

      ptr = memavail;
      for (j = 0; j < nvalues; j++) {
	k = order[j];
	memcpy(ptr,&sptr[soffset[k]],slength[k]);
	ptr += slength[k];
      }
      memcpy(multivalue,memavail,ptr-memavail);
    }

    // overwrite the changed KMV page

    kmv->overwrite_page(ipage);
  }

  // insure KMV file is closed, if last request_page() was for extended page

  kmv->close_file();

  memory->sfree(order);
  memory->sfree(soffset);

  stats("Sort_multivalues",0);

  uint64_t nkeyall;
  MPI_Allreduce(&kmv->nkmv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   sort keys or values in a KV to create a new KV
   flag = 0 = sort keys, flag = 1 = sort values
------------------------------------------------------------------------- */

void MapReduce::sort_kv(int flag)
{
  int i,j,n,nkey,keybytes,valuebytes,nspool,nentry;
  int dummy1,dummy2,dummy3;
  char *ptr,*key,*value;
  char *mem2a,*mem2b,*mem2c;
  int *order;
  Spool **spools,*sp;
  char *fname;

  char *page;
  int npage = kv->request_info(&page);

  // if multiple pages, setup spool files
  // partition mem2 into 3 pieces for spool merges

  if (npage > 1) {
    nspool = 2*npage - 1;
    spools = new Spool*[nspool];
    int memspool = memhalf/3/ALIGNFILE * ALIGNFILE;
    mem2a = mem2;
    mem2b = &mem2[memspool];
    mem2c = &mem2[2*memspool];
    for (int i = 0; i < nspool; i++) {
      fname = file_create(SORTFILE,i);
      spools[i] = new Spool(fname,memspool,memory,error);
      delete [] fname;
    }
  }

  // loop over pages, sort each by keys or values

  for (int ipage = 0; ipage < npage; ipage++) {

    nkey = kv->request_page(ipage,dummy1,dummy2,dummy3);

    // setup 3 int arrays from memavail (guaranteed to be large enough)
    // order = ordering of keys or values in KV, initially 0 to N-1
    // soffset = start of each key or value
    // slength = length of each key or value

    order = (int *) memavail;
    soffset = (int *) &memavail[nkey*sizeof(int)];
    slength = (int *) &memavail[2*nkey*sizeof(int)];

    ptr = page;

    for (i = 0; i < nkey; i++) {
      order[i] = i;
      
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
      
      if (flag == 0) {
	soffset[i] = key - page;
	slength[i] = keybytes;
      } else {
	soffset[i] = value - page;
	slength[i] = valuebytes;
      }
    }

    // sort keys or values via qsort()

    mrptr = this;
    sptr = page;
    qsort(order,nkey,sizeof(int),compare_standalone);

    // soffset = start of each KV pair
    // slength = length of entire KV pair

    char *ptr_start;
    ptr = page;

    for (i = 0; i < nkey; i++) {
      soffset[i] = ptr - page;
      
      ptr_start = ptr;
      keybytes = *((int *) ptr);
      valuebytes = *((int *) (ptr+sizeof(int)));;

      ptr += twolenbytes;
      ptr = ROUNDUP(ptr,kalignm1);
      ptr += keybytes;
      ptr = ROUNDUP(ptr,valignm1);
      ptr += valuebytes;
      ptr = ROUNDUP(ptr,talignm1);
      
      slength[i] = ptr - ptr_start;
    }

    // if single page, sort into mem2, copy back to page
    // if multiple pages, write page to a spool file

    if (npage == 1) {
      ptr = mem2;
      for (i = 0; i < nkey; i++) {
	j = order[i];
	memcpy(ptr,&sptr[soffset[j]],slength[j]);
	ptr += slength[j];
      }
      memcpy(page,mem2,ptr-mem2);

    } else {
      sp = spools[ipage];
      sp->assign(mem2a);
      for (i = 0; i < nkey; i++) {
	j = order[i];
	sp->add(slength[j],&sptr[soffset[j]]);
      }
      sp->complete();
    }
  }

  // if single page, all done

  if (npage == 1) return;

  // perform merge sort on pairs of spool files
  // assign 1/3 of mem2 to each spool in merge as in-memory page

  int isrc = 0;
  int idest = npage;

  for (i = 0; i < npage-1; i++) {
    spools[isrc]->assign(mem2a);
    spools[isrc+1]->assign(mem2b);
    spools[idest]->assign(mem2c);
    merge(flag,spools[isrc],spools[isrc+1],spools[idest]);
    spools[idest++]->complete();
    delete spools[isrc++];
    delete spools[isrc++];
  }

  // convert final spools[nspool-1] to a new KV

  delete kv;
  fname = file_create(KVFILE,instance_me);
  kv = new KeyValue(comm,memavail,memquarter,kalign,valign,fname);
  memswap(fname);

  sp = spools[nspool-1];
  sp->assign(mem2a);
  npage = sp->request_info(&page);

  for (int ipage = 0; ipage < npage; ipage++) {
    nentry = sp->request_page(ipage);
    kv->add(nentry,page);
  }

  kv->complete();

  // delete last spool file and data structure

  delete spools[nspool-1];
  delete [] spools;
}

/* ----------------------------------------------------------------------
   merge sort of 2 spool sources into a 3rd spool
   flag = 0 for key sort, flag = 1 for value sort
------------------------------------------------------------------------- */

void MapReduce::merge(int flag, Spool *s1, Spool *s2, Spool *dest)
{
  int result,ientry1,ientry2,nbytes1,nbytes2;
  char *str1,*str2;
  
  char *page1,*page2;
  int npage1 = s1->request_info(&page1);
  int npage2 = s2->request_info(&page2);

  int ipage1 = 0;
  int ipage2 = 0;
  int nentry1 = s1->request_page(ipage1);
  int nentry2 = s2->request_page(ipage2);
  ientry1 = ientry2 = 0;

  char *ptr1 = page1;
  char *ptr2 = page2;
  int len1 = extract(flag,ptr1,str1,nbytes1);
  int len2 = extract(flag,ptr2,str2,nbytes2);

  int done = 0;

  while (1) {
    if (done == 0) result = compare(str1,nbytes1,str2,nbytes2);

    if (result <= 0) {
      dest->add(len1,ptr1);
      ptr1 += len1;
      ientry1++;

      if (ientry1 == nentry1) {
	ipage1++;
	if (ipage1 < npage1) {
	  nentry1 = s1->request_page(ipage1);
	  ientry1 = 0;
	  ptr1 = page1;
	  len1 = extract(flag,ptr1,str1,nbytes1);
	} else {
	  done++;
	  if (done == 2) break;
	  result = 1;
	}
      } else len1 = extract(flag,ptr1,str1,nbytes1);
    }

    if (result >= 0) {
      dest->add(len2,ptr2);
      ptr2 += len2;
      ientry2++;

      if (ientry2 == nentry2) {
	ipage2++;
	if (ipage2 < npage2) {
	  nentry2 = s2->request_page(ipage2);
	  ientry2 = 0;
	  ptr2 = page2;
	  len2 = extract(flag,ptr2,str2,nbytes2);
	} else {
	  done++;
	  if (done == 2) break;
	  result = -1;
	}
      } else len2 = extract(flag,ptr2,str2,nbytes2);
    }
  }
}

/* ----------------------------------------------------------------------
   extract key from a Spool entry
   return key and keybytes
   also return byte increment to next entry
------------------------------------------------------------------------- */

int MapReduce::extract(int flag, char *ptr_start, char *&str, int &nbytes)
{
  char *ptr = ptr_start;
  int keybytes = *((int *) ptr);
  int valuebytes = *((int *) (ptr+sizeof(int)));;

  ptr += twolenbytes;
  ptr = ROUNDUP(ptr,kalignm1);
  char *key = ptr;
  ptr += keybytes;
  ptr = ROUNDUP(ptr,valignm1);
  char *value = ptr;
  ptr += valuebytes;
  ptr = ROUNDUP(ptr,talignm1);

  if (flag == 0) {
    str = key;
    nbytes = keybytes;
  } else {
    str = value;
    nbytes = valuebytes;
  }

  return ptr - ptr_start;
}

/* ----------------------------------------------------------------------
   wrappers on user-provided key or value comparison functions
   necessary so can extract 2 keys or values to pass back to application
   2-level wrapper needed b/c qsort() cannot be passed a class method
     unless it were static, but then it couldn't access MR class data
   so qsort() is passed standalone non-class method
   it accesses static class member mrptr, set before call to qsort()
   standalone calls back into class wrapper which calls user compare()
------------------------------------------------------------------------- */

int compare_standalone(const void *iptr, const void *jptr)
{
  return MapReduce::mrptr->compare_wrapper(*(int *) iptr,*(int *) jptr);
}

int MapReduce::compare_wrapper(int i, int j)
{
  return compare(&sptr[soffset[i]],slength[i],&sptr[soffset[j]],slength[j]);
}

/* ----------------------------------------------------------------------
   print stats for KV
------------------------------------------------------------------------- */

void MapReduce::kv_stats(int level)
{
  if (kv == NULL) error->all("Cannot print stats without KeyValue");

  uint64_t nkeyall,keysizeall,valuesizeall;
  MPI_Allreduce(&kv->nkv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  MPI_Allreduce(&kv->ksize,&keysizeall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  MPI_Allreduce(&kv->vsize,&valuesizeall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  double mbyte = 1024.0*1024.0;

  if (me == 0)
    printf("%lu pairs, %.3g Mb keys, %.3g Mb values\n",
	   nkeyall,keysizeall/mbyte,valuesizeall/mbyte);

  if (level == 2) {
    int histo[10],histotmp[10];
    double ave,max,min;
    double tmp = kv->nkv;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  KV pairs:   %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = kv->ksize/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Kdata (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = kv->vsize/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Vdata (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
  }
}

/* ----------------------------------------------------------------------
   print stats for KMV
------------------------------------------------------------------------- */

void MapReduce::kmv_stats(int level)
{
  if (kmv == NULL) error->all("Cannot print stats without KeyMultiValue");

  uint64_t nkeyall,keysizeall,valuesizeall;
  MPI_Allreduce(&kmv->nkmv,&nkeyall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  MPI_Allreduce(&kmv->ksize,&keysizeall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  MPI_Allreduce(&kmv->vsize,&valuesizeall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
  double mbyte = 1024.0*1024.0;

  if (me == 0)
    printf("%lu pairs, %.3g Mb keys, %.3g Mb values\n",
	   nkeyall,keysizeall/mbyte,valuesizeall/mbyte);

  if (level == 2) {
    int histo[10],histotmp[10];
    double ave,max,min;
    double tmp = kmv->nkmv;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  KMV pairs:  %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = kmv->ksize/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Kdata (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = kmv->vsize/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Vdata (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
  }
}

/* ----------------------------------------------------------------------
   print cummulative file read/write and comm stats
------------------------------------------------------------------------- */

void MapReduce::cummulative_stats(int level, int reset)
{
  int histo[10],histotmp[10];
  double tmp,ave,max,min;

  double mbyte = 1024.0*1024.0;

  // communication

  uint64_t csize[2] = {cssize,crsize};
  uint64_t allcsize[2] = {0,0};

  MPI_Allreduce(csize,allcsize,2,MPI_UNSIGNED_LONG,MPI_SUM,comm);

  if (me == 0) printf("Cummulative comm stats = %.3g Mb send, %.3g Mb recv\n", 
		      allcsize[0]/mbyte,allcsize[1]/mbyte);
  if (level == 2) {
    tmp = csize[0]/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Send (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = csize[1]/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Recv (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
  }

  // file I/O

  uint64_t size[6] = {KeyValue::rsize,KeyValue::wsize,
		      KeyMultiValue::rsize,KeyMultiValue::wsize,
		      Spool::rsize,Spool::wsize};
  uint64_t allsize[6] = {0,0,0,0,0,0};

  MPI_Allreduce(size,allsize,6,MPI_UNSIGNED_LONG,MPI_SUM,comm);

  if (me == 0) printf("Cummulative internal file I/O stats = "
		      "%.3g Mb read, %.3g Mb write\n", 
		      (allsize[0]+allsize[2]+allsize[4])/mbyte,
		      (allsize[1]+allsize[3]+allsize[5])/mbyte);
  if (level == 2) {
    tmp = (size[0]+size[2]+size[4])/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Read (Mb):  %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = (size[1]+size[3]+size[5])/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Write (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
  }

  if (me == 0) printf("  KV files: %.3g Mb read, %.3g Mb write\n",
		      allsize[0]/mbyte,allsize[1]/mbyte);
  if (level == 2) {
    tmp = size[0]/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("    Read (Mb):  %g ave %g max %g min\n",ave,max,min);
      printf("    Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = size[1]/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("    Write (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("    Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
  }
  if (me == 0) printf("  KMV files: %.3g Mb read, %.3g Mb write\n",
		      allsize[2]/mbyte,allsize[3]/mbyte);
  if (level == 2) {
    tmp = size[2]/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("    Read (Mb):  %g ave %g max %g min\n",ave,max,min);
      printf("    Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = size[3]/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("    Write (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("    Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
  }
  if (me == 0) printf("  Spool files: %.3g Mb read, %.3g Mb write\n",
		      allsize[4]/mbyte,allsize[5]/mbyte);
  if (level == 2) {
    tmp = size[4]/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("    Read (Mb):  %g ave %g max %g min\n",ave,max,min);
      printf("    Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = size[5]/mbyte;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("    Write (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("    Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
  }

  if (reset) {
    cssize = crsize = 0;
    KeyValue::rsize = 0;
    KeyValue::wsize = 0;
    KeyMultiValue::rsize = 0;
    KeyMultiValue::wsize = 0;
    Spool::rsize = 0;
    Spool::wsize = 0;
  }
}

/* ----------------------------------------------------------------------
   stats for one operation and its resulting KV or KMV
   which = 0 for KV, which = 1 for KMV
   output timer, KV/KMV, comm, I/O, or nothing depending on settings
------------------------------------------------------------------------- */

void MapReduce::stats(const char *heading, int which)
{
  if (timer) {
    if (timer == 1) {
      MPI_Barrier(comm);
      time_stop = MPI_Wtime();
      if (me == 0) printf("%s time (secs) = %g\n",
			  heading,time_stop-time_start);
    } else if (timer == 2) {
      time_stop = MPI_Wtime();
      int histo[10],histotmp[10];
      double ave,max,min;
      double tmp = time_stop-time_start;
      histogram(1,&tmp,ave,max,min,10,histo,histotmp);
      if (me == 0) {
	printf("%s time (secs) = %g ave %g max %g min\n",heading,ave,max,min);
	printf("  Histogram: ");
	for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
	printf("\n");
      }
    }
  }

  if (verbosity == 0) return;
  if (which == 0) {
    if (me == 0) printf("%s KV = ",heading);
    kv_stats(verbosity);
  } else {
    if (me == 0) printf("%s KMV = ",heading);
    kmv_stats(verbosity);
  }

  file_stats(1);

  if (cssize_one || crsize_one) {
    uint64_t rall,sall;
    MPI_Allreduce(&cssize_one,&sall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
    MPI_Allreduce(&crsize_one,&rall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
    double mbyte = 1024.0*1024.0;
    if (me == 0) printf("%s Comm = %.3g Mb send, %.3g Mb recv\n",heading,
			sall/mbyte,rall/mbyte);
    if (verbosity == 2) {
      int histo[10],histotmp[10];
      double ave,max,min;
      double tmp = cssize_one/mbyte;
      histogram(1,&tmp,ave,max,min,10,histo,histotmp);
      if (me == 0) {
	printf("  Send (Mb):  %g ave %g max %g min\n",ave,max,min);
	printf("  Histogram: ");
	for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
	printf("\n");
      }
      tmp = crsize_one/mbyte;
      histogram(1,&tmp,ave,max,min,10,histo,histotmp);
      if (me == 0) {
	printf("  Recv (Mb):  %g ave %g max %g min\n",ave,max,min);
	printf("  Histogram: ");
	for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
	printf("\n");
      }
    }
  }

  if (rsize_one || wsize_one) {
    uint64_t rall,wall;
    MPI_Allreduce(&rsize_one,&rall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
    MPI_Allreduce(&wsize_one,&wall,1,MPI_UNSIGNED_LONG,MPI_SUM,comm);
    double mbyte = 1024.0*1024.0;
    if (me == 0) printf("%s I/O = %.3g Mb read, %.3g Mb write\n",heading,
			rall/mbyte,wall/mbyte);
    if (verbosity == 2) {
      int histo[10],histotmp[10];
      double ave,max,min;
      double tmp = rsize_one/mbyte;
      histogram(1,&tmp,ave,max,min,10,histo,histotmp);
      if (me == 0) {
	printf("  Read (Mb):   %g ave %g max %g min\n",ave,max,min);
	printf("  Histogram: ");
	for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
	printf("\n");
      }
      tmp = wsize_one/mbyte;
      histogram(1,&tmp,ave,max,min,10,histo,histotmp);
      if (me == 0) {
	printf("  Write (Mb):  %g ave %g max %g min\n",ave,max,min);
	printf("  Histogram: ");
	for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
	printf("\n");
      }
    }
  }
}

/* ----------------------------------------------------------------------
   change fpath, but only if allocation has not occurred
------------------------------------------------------------------------- */

void MapReduce::set_fpath(char *str)
{
  if (allocated) return;

  delete [] fpath;
  int n = strlen(str) + 1;
  fpath = new char[n];
  strcpy(fpath,str);
}

/* ----------------------------------------------------------------------
   round N up to multiple of nalign and return it
------------------------------------------------------------------------- */

char *MapReduce::file_create(int flag, int counter)
{
  int n = strlen(fpath) + 32;
  char *fname = new char[n];
  if (flag == KVFILE) {
    if (memtoggle == 0)
      sprintf(fname,"%s/mrmpi.kva.%d.%d",fpath,counter,me);
    else
      sprintf(fname,"%s/mrmpi.kvb.%d.%d",fpath,counter,me);
  } else if (flag == KMVFILE) 
    sprintf(fname,"%s/mrmpi.kmv.%d.%d",fpath,counter,me);
  else if (flag == SORTFILE)
    sprintf(fname,"%s/mrmpi.sps.%d.%d",fpath,counter,me);
  return fname;
}

/* ----------------------------------------------------------------------
   size of file read/writes from KV, KMV, and Spool files
   flag = 0 -> rsize/wsize = current size
   flag = 1 -> rsize/wsize = current size - previous size
------------------------------------------------------------------------- */

void MapReduce::file_stats(int flag)
{
  if (flag == 0) {
    rsize_one = KeyValue::rsize + KeyMultiValue::rsize + Spool::rsize;
    wsize_one = KeyValue::wsize + KeyMultiValue::wsize + Spool::wsize;
    cssize_one = cssize;
    crsize_one = crsize;
  } else {
    rsize_one = KeyValue::rsize + KeyMultiValue::rsize + Spool::rsize - 
      rsize_one;
    wsize_one = KeyValue::wsize + KeyMultiValue::wsize + Spool::wsize -
      wsize_one;
    cssize_one = cssize - cssize_one;
    crsize_one = crsize - crsize_one;
  }
}

/* ---------------------------------------------------------------------- */

void MapReduce::start_timer()
{
  if (timer == 1) MPI_Barrier(comm);
  time_start = MPI_Wtime();
}

/* ----------------------------------------------------------------------
   round N up to multiple of nalign and return it
------------------------------------------------------------------------- */

int MapReduce::roundup(int n, int nalign)
{
  if (n % nalign == 0) return n;
  n = (n/nalign + 1) * nalign;
  return n;
}
