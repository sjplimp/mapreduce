/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

#include "mpi.h"
#include "string.h"
#include "stdio.h"
#include "stdlib.h"
#include "stdint.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "keymultivalue.h"
#include "irregular.h"
#include "hash.h"
#include "memory.h"
#include "error.h"

using namespace MAPREDUCE_NS;

int compare_keys_wrapper(const void *, const void *);
int compare_values_wrapper(const void *, const void *);

MapReduce *MapReduce::mrptr;

/*
    printf("AAA %d %d %d %d\n",me,kv->nkey,kv->keysize,kv->valuesize);
    for (int i = 0; i < kv->nkey; i++)
      printf("AAA KEYS %d\n",kv->keys[i]);
    for (int i = 0; i < kv->nkey; i++)
      printf("AAA VALS %d\n",kv->values[i]);
    for (int i = 0; i < kv->nkey; i++)
      printf("AAA KDAT %s\n",&kv->keydata[kv->keys[i]]);
    for (int i = 0; i < kv->nkey; i++)
      printf("AAA VDAT %d\n",*(int*)&kv->valuedata[kv->values[i]]);
*/

/* ---------------------------------------------------------------------- */

MapReduce::MapReduce(MPI_Comm caller)
{
  comm = caller;
  MPI_Comm_rank(comm,&me);
  MPI_Comm_size(comm,&nprocs);

  memory = new Memory(comm);
  error = new Error(comm);

  kv = NULL;
  kmv = NULL;

  mapstyle = 0;
  verbosity = 0;
}

/* ---------------------------------------------------------------------- */

MapReduce::~MapReduce()
{
  delete memory;
  delete error;
  delete kv;
  delete kmv;
}

/* ----------------------------------------------------------------------
   aggregate a KV across procs to create a new KV
   initially, key copies can exist on many procs
   after aggregation, all copies of key are on same proc
   performed via parallel distributed hashing
   hash = user hash function (NULL if not provided)
   requires irregular all2all communication
------------------------------------------------------------------------- */

int MapReduce::aggregate(int (*hash)(char *, int))
{
  if (kv == NULL) error->all("Cannot aggregate without KeyValue");
  if (nprocs == 1) return kv->nkey;

  delete kmv;
  kmv = NULL;

  KeyValue *kvnew = new KeyValue(comm);
  Irregular *irregular = new Irregular(comm);

  // hash each key to a proc ID
  // either use user-provided hash function or hashlittle()

  int nkey = kv->nkey;
  int *keys = kv->keys;
  int *values = kv->values;
  char *keydata = kv->keydata;

  int *proclist = new int[kv->nkey];

  for (int i = 0; i < nkey; i++) {
    char *key = &keydata[keys[i]];
    int keylen = keys[i+1] - keys[i];
    if (hash) proclist[i] = hash(key,keylen) % nprocs;
    else proclist[i] = hashlittle(key,keylen,nprocs) % nprocs;
  }

  // redistribute key sizes, key data, value sizes, value data
  // convert key/value offsets into sizes

  irregular->pattern(nkey,proclist);

  int *slength = proclist;
  for (int i = 0; i < nkey; i++) slength[i] = keys[i+1] - keys[i];

  int nbytes = irregular->size(sizeof(int));
  kvnew->nkey = kvnew->maxkey = nbytes / sizeof(int);
  kvnew->keys = (int *) memory->smalloc(nbytes,"keys");
  irregular->exchange((char *) slength,(char *) kvnew->keys);

  nbytes = irregular->size(slength,kv->keys,kvnew->keys);
  kvnew->keysize = kvnew->maxkeysize = nbytes;
  kvnew->keydata = (char *) memory->smalloc(nbytes,"keydata");
  irregular->exchange(kv->keydata,kvnew->keydata);

  for (int i = 0; i < nkey; i++) slength[i] = values[i+1] - values[i];

  nbytes = irregular->size(sizeof(int));
  kvnew->values = (int *) memory->smalloc(nbytes,"values");
  irregular->exchange((char *) slength,(char *) kvnew->values);

  nbytes = irregular->size(slength,kv->values,kvnew->values);
  kvnew->valuesize = kvnew->maxvaluesize = nbytes;
  kvnew->valuedata = (char *) memory->smalloc(nbytes,"valuedata");
  irregular->exchange(kv->valuedata,kvnew->valuedata);

  delete [] slength;
  delete irregular;

  // convert key/value sizes back into offsets

  nkey = kvnew->nkey;
  keys = kvnew->keys;
  values = kvnew->values;

  int keysize = 0;
  int valuesize = 0;
  int tmp;

  for (int i = 0; i < nkey; i++) {
    tmp = keys[i];
    keys[i] = keysize;
    keysize += tmp;
    tmp = values[i];
    values[i] = valuesize;
    valuesize += tmp;
  }

  delete kv;
  kv = kvnew;
  kv->complete();

  stats("Aggregate",0,verbosity);

  int nkeyall;
  MPI_Allreduce(&kv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   clone KV to KMV so that KMV pairs are one-to-one copies of KV pairs
   each proc clones only its data
   assume each KV key is unique, but is not required
------------------------------------------------------------------------- */

int MapReduce::clone()
{
  if (kv == NULL) error->all("Cannot clone without KeyValue");

  delete kmv;
  kmv = new KeyMultiValue(comm);

  kmv->cloned = 1;
  kmv->nkey = kv->nkey;

  stats("Clone",1,verbosity);

  int nkeyall;
  MPI_Allreduce(&kmv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   collapse KV into a KMV with a single key/value
   each proc collapses only its data
   new key = provided key name (same on every proc)
   new value = list of old key,value,key,value,etc
------------------------------------------------------------------------- */

int MapReduce::collapse(char *key, int keylen)
{
  if (kv == NULL) error->all("Cannot collapse without KeyValue");

  delete kmv;
  kmv = new KeyMultiValue(comm);

  kmv->collapsed = 1;
  kmv->singlekey = new char[keylen];
  memcpy(kmv->singlekey,key,keylen);
  kmv->nkey = 1;

  stats("Collapse",1,verbosity);

  int nkeyall;
  MPI_Allreduce(&kmv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   collate KV to create a KMV
   aggregate followed by a convert
   hash = user hash function (NULL if not provided)
------------------------------------------------------------------------- */

int MapReduce::collate(int (*hash)(char *, int))
{
  if (kv == NULL) error->all("Cannot collate without KeyValue");

  int verbosity_hold = verbosity;
  verbosity = 0;

  aggregate(hash);
  convert();

  verbosity = verbosity_hold;
  stats("Collate",1,verbosity);
 
  int nkeyall;
  MPI_Allreduce(&kmv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   compress KV to create a smaller KV
   duplicate keys are replaced with a single key/value
   each proc compresses only its data
   create a temporary KMV
   call appcompress() with each key/multivalue in KMV
   appcompress() returns single key/value to new KV
------------------------------------------------------------------------- */

int MapReduce::compress(void (*appcompress)(char *, int, char **,
					    KeyValue *, void *), void *ptr)
{
  if (kv == NULL) error->all("Cannot compress without KeyValue");

  delete kmv;
  KeyValue *kvnew = new KeyValue(comm);
  KeyMultiValue *kmv = new KeyMultiValue(comm);

  kmv->create(kv);
  callback(kv,kmv,kvnew,appcompress,ptr);

  delete kmv;
  kmv = NULL;
  delete kv;
  kv = kvnew;

  stats("Compress",0,verbosity);

  int nkeyall;
  MPI_Allreduce(&kv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   convert KV to KMV
   duplicate keys are replaced with a single key/multivalue
   each proc converts only its data
   new key = old unique key
   new multivalue = concatenated list of all values for that key in KV
------------------------------------------------------------------------- */

int MapReduce::convert()
{
  if (kv == NULL) error->all("Cannot convert without KeyValue");

  delete kmv;
  kmv = new KeyMultiValue(comm);

  kmv->create(kv);

  stats("Convert",1,verbosity);

  int nkeyall;
  MPI_Allreduce(&kmv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   gather a distributed KV to a new KV on fewer procs
   numprocs = # of procs new KV resides on (0 to numprocs-1)
------------------------------------------------------------------------- */

int MapReduce::gather(int numprocs)
{
  MPI_Status status;

  if (kv == NULL) error->all("Cannot gather without KeyValue");

  if (nprocs == 1 || numprocs >= nprocs) {
    int nkeyall;
    MPI_Allreduce(&kv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
    return nkeyall;
  }

  delete kmv;
  kmv = NULL;

  // lo procs collect key/value pairs from hi procs
  // lo procs are those with ID < numprocs
  // lo procs recv from set of hi procs with same (ID % numprocs)

  int flag,size;

  if (me < numprocs) {
    char *buf = NULL;
    int maxsize = 0;

    for (int iproc = me+numprocs; iproc < nprocs; iproc += numprocs) {
      MPI_Send(&flag,0,MPI_INT,iproc,0,comm);
      MPI_Recv(&size,1,MPI_INT,iproc,0,comm,&status);
      if (size > maxsize) {
	delete [] buf;
	buf = new char[size];
      }
      MPI_Recv(buf,size,MPI_BYTE,iproc,0,comm,&status);
      kv->unpack(buf);
    }
    
    delete [] buf;

  } else {
    char *buf;
    size = kv->pack(&buf);

    int iproc = me % numprocs;
    MPI_Recv(&flag,0,MPI_INT,iproc,0,comm,&status);
    MPI_Send(&size,1,MPI_INT,iproc,0,comm);
    MPI_Send(buf,size,MPI_BYTE,iproc,0,comm);

    delete [] buf;
    delete kv;
    kv = new KeyValue(comm);
  }

  kv->complete();

  stats("Gather",0,verbosity);

  int nkeyall;
  MPI_Allreduce(&kv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   create a KV via a parallel map operation for nmap tasks
   make one call to appmap() for each task
   mapstyle determines how tasks are partioned to processors
------------------------------------------------------------------------- */

int MapReduce::map(int nmap, void (*appmap)(int, KeyValue *, void *),
		   void *ptr, int addflag)
{
  MPI_Status status;

  delete kmv;
  kmv = NULL;

  if (addflag == 0) {
    delete kv;
    kv = new KeyValue(comm);
  } else if (kv == NULL) 
    kv = new KeyValue(comm);

  // nprocs = 1 = all tasks to single processor
  // mapstyle 0 = chunk of tasks to each proc
  // mapstyle 1 = strided tasks to each proc
  // mapstyle 2 = master/slave assignment of tasks

  if (nprocs == 1) {
    for (int itask = 0; itask < nmap; itask++) appmap(itask,kv,ptr);

  } else if (mapstyle == 0) {
    int lo = me * nmap / nprocs;
    int hi = (me+1) * nmap / nprocs;
    for (int itask = lo; itask < hi; itask++) appmap(itask,kv,ptr);

  } else if (mapstyle == 1) {
    for (int itask = me; itask < nmap; itask += nprocs) appmap(itask,kv,ptr);

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
	appmap(itask,kv,ptr);
	MPI_Send(&itask,1,MPI_INT,0,0,comm);
      }
    }

  } else error->all("Invalid mapstyle setting");

  kv->complete();
  stats("Map",0,verbosity);

  int nkeyall;
  MPI_Allreduce(&kv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   create a KV from a KMV via a parallel reduce operation for nmap tasks
   make one call to appreduce() for each KMV pair
   each proc processes its owned KMV pairs
------------------------------------------------------------------------- */

int MapReduce::reduce(void (*appreduce)(char *, int, char **,
					KeyValue *, void *), void *ptr)
{
  if (kmv == NULL) error->all("Cannot reduce without KeyMultiValue");

  KeyValue *kvnew = new KeyValue(comm);

  if (kmv->collapsed) {
    int nkey = kv->nkey;
    int *keys = kv->keys;
    int *values = kv->values;
    char *keydata = kv->keydata;
    char *valuedata = kv->valuedata;

    char **valueptrs = new char*[2*nkey];
    int nvalues = 0;
    for (int i = 0; i < kv->nkey; i++) {
      valueptrs[nvalues++] = &keydata[keys[i]];
      valueptrs[nvalues++] = &valuedata[values[i]];
    }
    appreduce(kmv->singlekey,nvalues,valueptrs,kvnew,ptr);
    delete [] valueptrs;

  } else if (kmv->cloned) {
    int nkey = kv->nkey;
    int *keys = kv->keys;
    int *values = kv->values;
    char *keydata = kv->keydata;
    char *valuedata = kv->valuedata;

    for (int i = 0; i < nkey; i++) {
      char *valueptr = &valuedata[values[i]];
      appreduce(&keydata[keys[i]],1,&valueptr,kvnew,ptr);
    }

  } else callback(kv,kmv,kvnew,appreduce,ptr);

  delete kmv;
  kmv = NULL;
  delete kv;
  kv = kvnew;
  kv->complete();

  stats("Reduce",0,verbosity);

  int nkeyall;
  MPI_Allreduce(&kv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   scrunch KV to create a KMV on fewer processors, each with a single pair
   gather followed by a collapse
   numprocs = # of procs new KMV resides on (0 to numprocs-1)
   new key = provided key name (same on every proc)
   new value = list of old key,value,key,value,etc
------------------------------------------------------------------------- */

int MapReduce::scrunch(int numprocs, char *key, int keylen)
{
  if (kv == NULL) error->all("Cannot scrunch without KeyValue");

  int verbosity_hold = verbosity;
  verbosity = 0;

  gather(numprocs);
  collapse(key,keylen);

  verbosity = verbosity_hold;
  stats("Scrunch",1,verbosity);

  int nkeyall;
  MPI_Allreduce(&kmv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   sort keys in a KV to create a new KV
   use appcompare() to compare 2 keys
   each proc sorts only its data
------------------------------------------------------------------------- */

int MapReduce::sort_keys(int (*appcompare)(char *, char *))
{
  if (kv == NULL) error->all("Cannot sort_keys without KeyValue");

  compare = appcompare;
  mrptr = this;
  sort_kv(0);

  stats("Sort_keys",0,verbosity);

  int nkeyall;
  MPI_Allreduce(&kv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   sort values in a KV to create a new KV
   use appcompare() to compare 2 values
   each proc sorts only its data
------------------------------------------------------------------------- */

int MapReduce::sort_values(int (*appcompare)(char *, char *))
{
  if (kv == NULL) error->all("Cannot sort_values without KeyValue");

  compare = appcompare;
  mrptr = this;
  sort_kv(1);

  stats("Sort_values",0,verbosity);

  int nkeyall;
  MPI_Allreduce(&kv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   sort values within each multivalue in a KMV, does not create a new KMV
   use appcompare() to compare 2 values within a multivalue
   each proc sorts only its data
------------------------------------------------------------------------- */

int MapReduce::sort_multivalues(int (*appcompare)(char *, char *))
{
  if (kmv == NULL) error->all("Cannot sort_multivalues without KeyMultiValue");

  if (kmv->cloned || kmv->collapsed) {
    int nkeyall;
    MPI_Allreduce(&kmv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
    return nkeyall;
  }

  int nkey = kmv->nkey;
  KeyMultiValue::KeyEntry *unique = kmv->keys;
  int *valueindex = kmv->valueindex;

  // valueindices = initial ordering of values in multi-value for one KMV key
  // will get reordered by qsort
  // use reordered valueindices to reset valueindex
  // is an in-place sort of the values in the multi-value

  int *valueindices = NULL;
  int maxvalue = 0;

  for (int i = 0; i < nkey; i++) {
    int nvalues = unique[i].count;
    int vindex = unique[i].valuehead;
    
    if (nvalues > maxvalue) {
      maxvalue = nvalues;
      delete [] valueindices;
      valueindices = new int[nvalues];
    }
    
    for (int j = 0; j < nvalues; j++) {
      valueindices[j] = vindex;
      vindex = valueindex[vindex];
    }

    compare = appcompare;
    mrptr = this;
    qsort(valueindices,nvalues,sizeof(int),compare_values_wrapper);

    unique[i].valuehead = valueindices[0];
    for (int j = 1; j < nvalues; j++) {
      valueindex[valueindices[j-1]] = valueindices[j];
    }
    valueindex[valueindices[nvalues-1]] = -1;
  }

  delete [] valueindices;

  stats("Sort_multivalues",0,verbosity);

  int nkeyall;
  MPI_Allreduce(&kmv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  return nkeyall;
}

/* ----------------------------------------------------------------------
   print stats for KV
------------------------------------------------------------------------- */

void MapReduce::kv_stats(int level)
{
  if (kv == NULL) error->all("Cannot print stats without KeyValue");

  int nkeyall;
  MPI_Allreduce(&kv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  double keysize = kv->keysize;
  double keysizeall;
  MPI_Allreduce(&keysize,&keysizeall,1,MPI_DOUBLE,MPI_SUM,comm);
  double valuesize = kv->valuesize;
  double valuesizeall;
  MPI_Allreduce(&valuesize,&valuesizeall,1,MPI_DOUBLE,MPI_SUM,comm);

  if (me == 0)
    printf("%d key/value pairs, %.1g Mb of key data, %.1g Mb of value data\n",
	   nkeyall,keysizeall/1024.0/1024.0,valuesizeall/1024.0/1024.0);

  if (level == 2) {
    int histo[10],histotmp[10];
    double ave,max,min;
    double tmp = kv->nkey;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  KV pairs:   %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = kv->keysize/1024.0/1024.0;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Kdata (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = kv->valuesize/1024.0/1024.0;
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

  int nkeyall;
  MPI_Allreduce(&kmv->nkey,&nkeyall,1,MPI_INT,MPI_SUM,comm);
  double keysize = kv->keysize;
  double keysizeall;
  MPI_Allreduce(&keysize,&keysizeall,1,MPI_DOUBLE,MPI_SUM,comm);
  double valuesize = kv->valuesize;
  double valuesizeall;
  MPI_Allreduce(&valuesize,&valuesizeall,1,MPI_DOUBLE,MPI_SUM,comm);

  if (me == 0)
    printf("%d key/multi-value pairs, "
	   "%.1g Mb of key data, %.1g Mb of value data\n",
	   nkeyall,keysizeall/1024.0/1024.0,valuesizeall/1024.0/1024.0);

  if (level == 2) {
    int histo[10],histotmp[10];
    double ave,max,min;
    double tmp = kmv->nkey;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  KMV pairs:  %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = kv->keysize/1024.0/1024.0;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Kdata (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = kv->valuesize/1024.0/1024.0;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Vdata (Mb): %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
    tmp = kmv->maxdepth;
    histogram(1,&tmp,ave,max,min,10,histo,histotmp);
    if (me == 0) {
      printf("  Max bucket: %g ave %g max %g min\n",ave,max,min);
      printf("  Histogram: ");
      for (int i = 0; i < 10; i++) printf(" %d",histo[i]);
      printf("\n");
    }
  }
}

/* ----------------------------------------------------------------------
   sort values in a KV to create a new KV
   use appcompare() to compare 2 values
   each proc sorts only its data
------------------------------------------------------------------------- */

void MapReduce::sort_kv(int flag)
{
  delete kmv;
  kmv = NULL;

  int nkey = kv->nkey;
  int *keys = kv->keys;
  int *values = kv->values;
  char *keydata = kv->keydata;
  char *valuedata = kv->valuedata;

  // order = ordering of keys in KV, initially 0 to N-1
  // will get reordered by qsort
  // use reordered order array to build a new KV, one key/value at a time

  int *order = new int[nkey];
  for (int i = 0; i < nkey; i++) order[i] = i;

  if (flag == 0) qsort(order,nkey,sizeof(int),compare_keys_wrapper);
  else qsort(order,nkey,sizeof(int),compare_values_wrapper);

  KeyValue *kvnew = new KeyValue(comm);
  for (int i = 0; i < nkey; i++) {
    char *key = &keydata[keys[order[i]]];
    int keylen = keys[order[i]+1] - keys[order[i]];
    char *value = &valuedata[values[order[i]]];
    int valuelen = values[order[i]+1] - values[order[i]];
    kvnew->add(key,keylen,value,valuelen);
  }

  delete [] order;
  delete kv;
  kv = kvnew;
  kv->complete();
}

/* ----------------------------------------------------------------------
   wrappers on user-provided key comparison function
   necessary so can extract 2 keys to pass back to application
   2-level wrapper needed b/c qsort() cannot be passed a class method
     unless it were static, but then it couldn't access MR class data
   so qsort() is passed compare_keys_wrapper, which is non-class method
   it accesses static class member mrptr, set before qsort() call
   compare_keys_wrapper calls back into class which calls user compare()
------------------------------------------------------------------------- */

int compare_keys_wrapper(const void *iptr, const void *jptr)
{
  return MapReduce::mrptr->compare_keys(*(int *) iptr,*(int *) jptr);
}

int MapReduce::compare_keys(int i, int j)
{
  return compare(&kv->keydata[kv->keys[i]],&kv->keydata[kv->keys[j]]);
}

/* ----------------------------------------------------------------------
   wrappers on user-provided value comparison function
   necessary so can extract 2 values to pass back to application
   2-level wrapper needed b/c qsort() cannot be passed a class method
     unless it were static, but then it couldn't access MR class data
   so qsort() is passed compare_values_wrapper, which is non-class method
   it accesses static class member mrptr, set before qsort() call
   compare_values_wrapper calls back into class which calls user compare()
------------------------------------------------------------------------- */

int compare_values_wrapper(const void *iptr, const void *jptr)
{
  return MapReduce::mrptr->compare_values(*(int *) iptr,*(int *) jptr);
}

int MapReduce::compare_values(int i, int j)
{
  return compare(&kv->valuedata[kv->values[i]],&kv->valuedata[kv->values[j]]);
}

/* ----------------------------------------------------------------------
   stats for either KV or KMV
------------------------------------------------------------------------- */

void MapReduce::stats(char *heading, int which, int level)
{
  if (level == 0) return;
  if (me == 0) printf("%s: ",heading);
  if (which == 0) kv_stats(level);
  else kmv_stats(level);
}

/* ----------------------------------------------------------------------
   perform a series of callbacks on KMV pairs
   used by compress() and reduce()
------------------------------------------------------------------------- */

void MapReduce::callback(KeyValue *kv1, KeyMultiValue *kmv1, KeyValue *kv2,
			 void (*func)(char *, int, char **,
				      KeyValue *, void *), void *ptr)
{
  int ncallback = kmv1->nkey;
  KeyMultiValue::KeyEntry *unique = kmv1->keys;
  int *valueindex = kmv1->valueindex;

  int *keys = kv1->keys;
  int *values = kv1->values;
  char *keydata = kv1->keydata;
  char *valuedata = kv1->valuedata;

  // build list of multi-value ptrs to pass with each callback invocation
  // could pass an interator instead

  char **valueptrs = NULL;
  int maxvalue = 0;

  for (int i = 0; i < ncallback; i++) {
    int keyindex = unique[i].keyindex;
    int nvalues = unique[i].count;
    int vindex = unique[i].valuehead;
    
    if (nvalues > maxvalue) {
      maxvalue = nvalues;
      delete [] valueptrs;
      valueptrs = new char*[nvalues];
    }
    
    for (int j = 0; j < nvalues; j++) {
      valueptrs[j] = &valuedata[values[vindex]];
      vindex = valueindex[vindex];
    }
    
    func(&keydata[keys[keyindex]],nvalues,valueptrs,kv2,ptr);
  }

  delete [] valueptrs;
}

/* ---------------------------------------------------------------------- */

void MapReduce::histogram(int n, double *data, 
			  double &ave, double &max, double &min,
			  int nhisto, int *histo, int *histotmp)
{
  min = 1.0e20;
  max = -1.0e20;
  ave = 0.0;
  for (int i = 0; i < n; i++) {
    ave += data[i];
    if (data[i] < min) min = data[i];
    if (data[i] > max) max = data[i];
  }

  int ntotal;
  MPI_Allreduce(&n,&ntotal,1,MPI_INT,MPI_SUM,comm);
  double tmp;
  MPI_Allreduce(&ave,&tmp,1,MPI_DOUBLE,MPI_SUM,comm);
  ave = tmp/ntotal;
  MPI_Allreduce(&min,&tmp,1,MPI_DOUBLE,MPI_MIN,comm);
  min = tmp;
  MPI_Allreduce(&max,&tmp,1,MPI_DOUBLE,MPI_MAX,comm);
  max = tmp;

  for (int i = 0; i < nhisto; i++) histo[i] = 0;

  int m;
  double del = max - min;
  for (int i = 0; i < n; i++) {
    if (del == 0.0) m = 0;
    else m = static_cast<int> ((data[i]-min)/del * nhisto);
    if (m > nhisto-1) m = nhisto-1;
    histo[m]++;
  }

  MPI_Allreduce(histo,histotmp,nhisto,MPI_INT,MPI_SUM,comm);
  for (int i = 0; i < nhisto; i++) histo[i] = histotmp[i];
}
