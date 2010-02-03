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

// MapReduce test_big program in C++
// Syntax: test_big -n N -m memsize
// (1) generates #processor * 2^N words; uses MR memsize = M.
// (2) assigns words to processors (key = processor ID; value = word)
// (3) collates by processor
// (4) performs wordcount with local compress followed by global reduce.

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "sys/stat.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.hpp"

using namespace MAPREDUCE_NS;

void fileread(int, KeyValue *, void *);
void fileread_thenbalance(int, KeyValue *, void *);
void genwords_fileequiv(int, KeyValue *, void *);
void genwords_wordpertask_thenbalance(int, KeyValue *, void *);
void genwords_wordpertask(int, KeyValue *, void *);
void genwords_thenbalance(int, KeyValue *, void *);
void balance(char *, int, char *, int, int *, KeyValue *, void *);
int identity(char *, int);
void sum(char *, int, char *, int, int *, KeyValue *, void *);
void globalsum(char *, int, char *, int, int *, KeyValue *, void *);
int ncompare(char *, int, char *, int);
void output(int, char *, int, char *, int, KeyValue *, void *);

struct Count {
  int n,limit,flag;
};

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);
  greetings();

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  if (me == 0) {
    for (int i=0; i < narg; i++) printf("%s ", args[i]);
    printf("\n");
    fflush(stdout);
  }

  if (narg <= 4 || 
     ((strcmp(args[1], "-n") != 0) && (strcmp(args[3], "-n") != 0)) || 
     ((strcmp(args[1], "-m") != 0) && (strcmp(args[3], "-m") != 0))) {
    if (me == 0) printf("Syntax: %s -n N -m memsize\n", args[0]);
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
// mr->verbosity = 1;
// mr->timer = 1;

  int N = atoi(args[2]);
#ifdef NEW_OUT_OF_CORE
  mr->memsize=atoi(args[4]);
#endif

  if (me == 0) {printf("Beginning map...\n"); fflush(stdout);}
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // Automatically generate nprocs*2^N words 
  uint64_t nwords = mr->map(nprocs, &genwords_thenbalance, &N);

  // Redistribute the words equally among the processors.  
  if (me == 0) {printf("Beginning balance...\n"); fflush(stdout);}
  mr->collate(&identity);  // Collates by processor
  mr->reduce(&balance, NULL);

  if (me == 0) {printf("Beginning wordcount...\n"); fflush(stdout);}

#define LOCALCOMPRESS
#ifdef LOCALCOMPRESS
  // Perform a local compression of the data first to reduce the amount
  // of communication that needs to be done.
  // This compression builds local hash tables, and requires an additional
  // pass over the data..
  uint64_t nunique = mr->compress(&sum,NULL);  // Do word-count locally first
  if (nprocs > 1) {
    mr->collate(NULL);        // Hash keys and local counts to processors
    nunique = mr->reduce(&globalsum,NULL); // Compute global sums for each key
  }
#else
  // Do not do a local compression but, rather, do a global aggregate only.
  mr->collate(NULL);
  uint64_t nunique = mr->reduce(&sum,NULL);
#endif

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0) printf("nwords = %llu : nunique = %llu : Time = %f\n", nwords, nunique, tstop - tstart);

#ifdef NEW_OUT_OF_CORE
  mr->total_stats(false);
#endif
  delete mr;

  MPI_Finalize();
}

///////////////////////////////////////////////////////////////////////////
void genwords_thenbalance(int itask, KeyValue *kv, void *ptr)
{
  // Compute number of instances of the word to emit.
  int N = *((int *) ptr);
  uint64_t nwords = (1 << N);

  int nprocs;
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  for (uint64_t i = 0; i < nwords; i++) {
    int proc = i % nprocs;
    char key[32];
    sprintf(key, "%llu", i);
    kv->add((char *) &proc, sizeof(int), key, strlen(key));
  }
}

///////////////////////////////////////////////////////////////////////////
int identity(char *key, int keybytes)
{
  int p = *((int *) key);
  return p;
}


///////////////////////////////////////////////////////////////////////////
void balance(char *key, int keybytes, char *multivalue,
	     int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  int offset = 0;
  for (int i = 0; i < nvalues; i++) {
    char *key = multivalue + offset;
    kv->add(key, valuebytes[i], NULL, 0);
    offset += valuebytes[i];
  }

  END_BLOCK_LOOP
}

/* ----------------------------------------------------------------------
   count word occurrence
   emit key = word, value = # of multi-values
------------------------------------------------------------------------- */

void sum(char *key, int keybytes, char *multivalue,
	 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,(char *) &nvalues,sizeof(int));
}

/* ----------------------------------------------------------------------
   count word occurrence
   emit key = word, value = sum of multi-values
------------------------------------------------------------------------- */

void globalsum(char *key, int keybytes, char *multivalue,
	 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int sum = 0; 
  for (int i = 0; i < nvalues; i++) sum += *(((int *) multivalue)+i);
  kv->add(key,keybytes,(char *) &sum,sizeof(int));
}

