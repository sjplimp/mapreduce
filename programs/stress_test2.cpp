// MapReduce stress_test2 program in C++
// A program to stress functions for large data sizes in MR-MPI.
// Probably shouldn't be used for timing, as there are many diagnostics
// collected and printed.
//
// Syntax: stress_test2 -n N -m memsize
//
// (1) Generates (N+1)*2^N words using Jon's power-law numbers. 
//     uses MR memsize = M.
//     Words are numbers in range [0:2^(N+1)-2] represented as strings.
//     Words are distributed so that there are 2^(N-k) occurrences of words
//     in [2^k - 1 : 2^(k+1) - 2], k = 0, 1, ..., N.
// (2) Assigns words to processors (key = processor ID; value = word)
// (3) Collates by processor
//     Tests lots of data re-shuffling.
// (4) Reduce emits words on each processor.
//     Tests large multivalue for a single key.  
// (5) Perform local wordcount with compress.
//     Note that the local compress is usually a good idea, but for the
//     input of this problem, it has no effect.
//     This operation can be skipped by undefining LOCALCOMPRESS below.
// (6) Perform global wordcount with collate followed by reduce.
//     Tests reduce with many keys with small multivalues.
//
//  Output should be
//  nwords = (N+1)*2^N : nunique = 2^(N+1) - 1 :  time

#define LOCALCOMPRESS

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.h"
#include "localdisks.hpp"
#include "genPowerLaw.hpp"
#include <assert.h>

using namespace MAPREDUCE_NS;

void sum(char *, int, char *, int, int *, KeyValue *, void *);
void globalsum(char *, int, char *, int, int *, KeyValue *, void *);


static int ME;
static uint64_t FREQ;

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);
  greetings();

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);
  ME = me;

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
  mr->verbosity = 1;
// mr->timer = 1;

  int N = atoi(args[2]);
  if (N > 3) FREQ = (1 << (N-3))/nprocs;
  else FREQ = 1;
  mr->memsize=atoi(args[4]);

  if (me == 0) {printf("KDD: genwords...map\n"); fflush(stdout);}
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // Automatically generate words.
  uint64_t nwords = mr->map(nprocs, &genPowerLaw_Redistrib, &N);

  // Redistribute the words equally among the processors.  
  if (me == 0) {printf("KDD:  redistribute...collate\n"); fflush(stdout);}
  mr->collate(&genPowerLaw_IdentityHash);  // Collates by processor
  if (me == 0) {printf("KDD:  redistribute...reduce\n"); fflush(stdout);}
  mr->reduce(&genPowerLaw_RedistributeReduce, NULL);

#ifdef LOCALCOMPRESS
  // Perform a local compression of the data first to reduce the amount
  // of communication that needs to be done.
  // This compression builds local hash tables, and requires an additional
  // pass over the data..
  if (me == 0) {printf("KDD:  wordcount...compress\n"); fflush(stdout);}
  uint64_t nunique = mr->compress(&sum,NULL);  // Do word-count locally first
  if (nprocs > 1) {
    if (me == 0) {printf("KDD:  wordcount...collate\n"); fflush(stdout);}
    mr->collate(NULL);        // Hash keys and local counts to processors
    if (me == 0) {printf("KDD:  wordcount...reduce\n"); fflush(stdout);}
    nunique = mr->reduce(&globalsum,NULL); // Compute global sums for each key
  }
#else
  // Do not do a local compression but, rather, do a global aggregate only.
  if (me == 0) {printf("KDD:  wordcount...collate\n"); fflush(stdout);}
  mr->collate(NULL);
  if (me == 0) {printf("KDD:  wordcount...reduce\n"); fflush(stdout);}
  uint64_t nunique = mr->reduce(&sum,NULL);
#endif

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0) {printf("KDD:  sanity test...clone\n"); fflush(stdout);}
  mr->clone();
  if (me == 0) {printf("KDD:  sanity test...reduce\n"); fflush(stdout);}
  genPowerLaw_SanityStruct a(N);
  uint64_t gerrors = 0;
  mr->reduce(&genPowerLaw_SanityTest, (void *) &a);
  MPI_Allreduce(&(a.nerrors), &gerrors, 1, MPI_UNSIGNED_LONG,
                MPI_SUM, MPI_COMM_WORLD);
  if (me == 0)
    if (gerrors > 0) printf("SANITY TEST FAILED: %llu ERRORS\n", gerrors);
    else printf("Sanity test OK.\n");


  if (me == 0) printf("nwords = %llu : nunique = %llu : Time = %f\n", nwords, nunique, tstop - tstart);

  mr->cummulative_stats(2, 0);
  delete mr;

  MPI_Finalize();
}

/* ----------------------------------------------------------------------
   count word occurrence
   emit key = word, value = # of multi-values
------------------------------------------------------------------------- */

void sum(char *key, int keybytes, char *multivalue,
         int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  static uint64_t progress = 0, nextprint = FREQ;
  progress++;
  if (ME == 0 && progress > nextprint) {
    printf("%d     sum %llu\n", ME, progress);
    nextprint += FREQ;
  }
  uint64_t total_nvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, total_nvalues)
  kv->add(key,keybytes,(char *) &total_nvalues,sizeof(uint64_t));
}

/* ----------------------------------------------------------------------
   count word occurrence
   emit key = word, value = sum of multi-values
------------------------------------------------------------------------- */

void globalsum(char *key, int keybytes, char *multivalue,
         int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  uint64_t sum = 0;
  static uint64_t progress = 0, nextprint = FREQ;
  progress++;
  if (ME == 0 && progress > nextprint) {
    printf("%d     globalsum %llu\n", ME, progress);
    nextprint += FREQ;
  }

  uint64_t total_nvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, total_nvalues)

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  for (int i = 0; i < nvalues; i++) sum += *(((uint64_t *) multivalue)+i);

  END_BLOCK_LOOP

  kv->add(key,keybytes,(char *) &sum,sizeof(uint64_t));
}

