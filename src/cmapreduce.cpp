/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

// C interface to MapReduce library
// ditto for Fortran, scripting language, or other hi-level languages

#include "cmapreduce.h"
#include "mapreduce.h"
#include "keyvalue.h"

using namespace MAPREDUCE_NS;

void *MR_create(MPI_Comm comm)
{
  MapReduce *mr = new MapReduce(comm);
  return (void *) mr;
}

void MR_destroy(void *MRptr)
{
  MapReduce *mr = (MapReduce *) MRptr;
  delete mr;
}

int MR_aggregate(void *MRptr, int (*myhash)(char *, int))
{
  MapReduce *mr = (MapReduce *) MRptr;
  return mr->aggregate(myhash);
}

int MR_clone(void *MRptr)
{
  MapReduce *mr = (MapReduce *) MRptr;
  return mr->clone();
}

int MR_collapse(void *MRptr, char *key, int keylen)
{
  MapReduce *mr = (MapReduce *) MRptr;
  return mr->collapse(key,keylen);
}

int MR_collate(void *MRptr, int (*myhash)(char *, int))
{
  MapReduce *mr = (MapReduce *) MRptr;
  return mr->collate(myhash);
}

int MR_compress(void *MRptr,
		void (*mycompress)(char *, int, char **, void *, void *),
		void *APPptr)
{
  typedef void (CompressFunc)(char *, int, char **, KeyValue *, void *);
  MapReduce *mr = (MapReduce *) MRptr;
  CompressFunc *appcompress = (CompressFunc *) mycompress;
  return mr->compress(appcompress,APPptr);
}

int MR_convert(void *MRptr)
{
  MapReduce *mr = (MapReduce *) MRptr;
  return mr->convert();
}

int MR_gather(void *MRptr, int numprocs)
{
  MapReduce *mr = (MapReduce *) MRptr;
  return mr->gather(numprocs);
}

int MR_map(void *MRptr, int nmap,
	   void (*mymap)(int, void *, void *),
	   void *APPptr)
{
  typedef void (MapFunc)(int, KeyValue *, void *);
  MapReduce *mr = (MapReduce *) MRptr;
  MapFunc *appmap = (MapFunc *) mymap;
  return mr->map(nmap,appmap,APPptr);
}

int MR_reduce(void *MRptr,
	      void (*myreduce)(char *, int, char **, void *, void *),
	      void *APPptr)
{
  typedef void (ReduceFunc)(char *, int, char **, KeyValue *, void *);
  MapReduce *mr = (MapReduce *) MRptr;
  ReduceFunc *appreduce = (ReduceFunc *) myreduce;
  return mr->reduce(appreduce,APPptr);
}

int MR_scrunch(void *MRptr, int numprocs, char *key, int keylen)
{
  MapReduce *mr = (MapReduce *) MRptr;
  return mr->scrunch(numprocs,key,keylen);
}

int MR_sort_keys(void *MRptr, int (*mycompare)(char *, char *))
{
  MapReduce *mr = (MapReduce *) MRptr;
  return mr->sort_keys(mycompare);
}

int MR_sort_values(void *MRptr, int (*mycompare)(char *, char *))
{
  MapReduce *mr = (MapReduce *) MRptr;
  return mr->sort_values(mycompare);
}

int MR_sort_multivalues(void *MRptr, int (*mycompare)(char *, char *))
{
  MapReduce *mr = (MapReduce *) MRptr;
  return mr->sort_multivalues(mycompare);
}

void MR_kv_add(void *KVptr, char *key, int keylen, char *value, int valuelen)
{
  KeyValue *kv = (KeyValue *) KVptr;
  kv->add(key,keylen,value,valuelen);
}

void MR_kv_add_multi_static(void *KVptr, int n, 
			    char *key, int keylen, char *value, int valuelen)
{
  KeyValue *kv = (KeyValue *) KVptr;
  kv->add(n,key,keylen,value,valuelen);
}

void MR_kv_add_multi_vary(void *KVptr, int n, 
			  char *key, int *keylen, char *value, int *valuelen)
{
  KeyValue *kv = (KeyValue *) KVptr;
  kv->add(n,key,keylen,value,valuelen);
}

void MR_kv_stats(void *MRptr, int level)
{
  MapReduce *mr = (MapReduce *) MRptr;
  mr->kv_stats(level);
}

void MR_kmv_stats(void *MRptr, int level)
{
  MapReduce *mr = (MapReduce *) MRptr;
  mr->kmv_stats(level);
}

void MR_set_mapstyle(void *MRptr, int value)
{
  MapReduce *mr = (MapReduce *) MRptr;
  mr->mapstyle = value;
}

void MR_set_verbosity(void *MRptr, int value)
{
  MapReduce *mr = (MapReduce *) MRptr;
  mr->verbosity = value;
}
