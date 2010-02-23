// MapReduce stress_test1 program in C++
// A program to stress functions for large data sizes in MR-MPI.
// Probably shouldn't be used for timing, as there are many diagnostics
// collected and printed.
//
// Syntax: stress_test1 -n N -m memsize
//
// (1) Generates #processor * 2^N words; uses MR memsize = M.
//     Words are the numbers 0 to 2^N-1 represented as strings.
// (2) Assigns words to processors (key = processor ID; value = word)
// (3) Collates by processor
//     Tests lots of data re-shuffling; only (2^N)/P words remain on
//     each processor; the rest move to new processors.
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
//  nwords = #proc * 2^N : nunique = 2^N :  time

#define LOCALCOMPRESS

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.hpp"
#include <assert.h>

using namespace MAPREDUCE_NS;

void genwords(int, KeyValue *, void *);
void redistribute_by_processor(char *, int, char *, int, int *, 
                               KeyValue *, void *);
int identity(char *, int);
void sum(char *, int, char *, int, int *, KeyValue *, void *);
void globalsum(char *, int, char *, int, int *, KeyValue *, void *);
void sanitytest(char *, int, char *, int, int *, KeyValue *, void *);


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
#ifdef NEW_OUT_OF_CORE
  mr->set_fpath(MYLOCALDISK); 
#endif
  mr->verbosity = 2;
  mr->timer = 1;

  int N = atoi(args[2]);
  if (N > 3) FREQ = (1 << (N-3));
  else FREQ = 1;
#ifdef NEW_OUT_OF_CORE
  mr->memsize=atoi(args[4]);
#endif

  if (me == 0) {printf("KDD: genwords...map\n"); fflush(stdout);}
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // Automatically generate nprocs*2^N words 
  uint64_t nwords = mr->map(nprocs, &genwords, &N);

  // Redistribute the words equally among the processors.  
  if (me == 0) {printf("KDD:  redistribute...collate\n"); fflush(stdout);}
  //mr->collate(&identity);  // Collates by processor
  mr->aggregate(&identity);
  mr->convert();
  if (me == 0) {printf("KDD:  redistribute...reduce\n"); fflush(stdout);}
  mr->reduce(&redistribute_by_processor, NULL);

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
  uint64_t errors = 0, gerrors = 0;
  //mr->reduce(&sanitytest, (void *) &errors);
  //MPI_Allreduce(&errors, &gerrors, 1, MPI_UNSIGNED_LONG,
  //              MPI_SUM, MPI_COMM_WORLD);
  if (me == 0) 
    if (gerrors > 0) printf("SANITY TEST FAILED: %llu ERRORS\n", gerrors);
    else printf("Sanity test OK.\n");

  if (me == 0) 
    printf("nwords = %llu : nunique = %llu : Time = %f\n", 
            nwords, nunique, tstop - tstart);

#ifdef NEW_OUT_OF_CORE
  mr->cummulative_stats(2, 0);
#endif
  delete mr;

  MPI_Finalize();
}

///////////////////////////////////////////////////////////////////////////
void genwords(int itask, KeyValue *kv, void *ptr)
{
  // Compute number of instances of the word to emit.
  int N = *((int *) ptr);
  uint64_t nwords = (1 << N);
  static uint64_t progress = 0, nextprint = FREQ;

  int nprocs, me;
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);

  for (uint64_t i = 0; i < nwords; i++) {
//    int proc = (i+me) % nprocs;
    int proc = i % nprocs;
    char key[32];
    sprintf(key, "%llu", i);
    kv->add((char *) &proc, sizeof(int), key, strlen(key)+1);
    progress++;
    if (ME == 0 && progress > nextprint) {
      printf("%d     genwords %llu\n", ME, progress);
      nextprint += FREQ;
    }
  }
}

///////////////////////////////////////////////////////////////////////////
int identity(char *key, int keybytes)
{
  int p = *((int *) key);
  static uint64_t progress = 0, nextprint = FREQ;
  progress++;
  if (ME == 0 && progress > nextprint) {
    printf("%d     identity %llu\n", ME, progress);
    nextprint += FREQ;
  }
  return p;
}


///////////////////////////////////////////////////////////////////////////
void redistribute_by_processor(char *key, int keybytes, char *multivalue,
	     int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  static int ncalls = 0;
  static uint64_t progress = 0;

  uint64_t total_nvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, total_nvalues)
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  char *key = multivalue;
  for (int i = 0; i < nvalues; i++) {
    kv->add(key, valuebytes[i], NULL, 0);
    key += valuebytes[i];
  }

  progress += nvalues;
  if (ME == 0) printf("%d     redistribute %llu words...\n", ME, progress);

  END_BLOCK_LOOP

  ncalls++;
  assert(ncalls == 1);  // redistribute_by_processor should be called 
                        // only once per processor.
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

/* ----------------------------------------------------------------------
   check the result; all words should have count = nprocs
   input:  KEY = key  MV = occurrence count for key
   emit nothing; return error result in ptr.
------------------------------------------------------------------------- */

void sanitytest(char *key, int keybytes, char *multivalue,
	 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int nprocs;
  uint64_t *error = (uint64_t *) ptr;
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  uint64_t total_nvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, total_nvalues)
  assert(total_nvalues == 1);

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  if (*((uint64_t*)multivalue) != nprocs) (*error)++;

  END_BLOCK_LOOP
}
