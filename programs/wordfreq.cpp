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

// MapReduce word frequency example in C++
// Syntax: wordfreq file1 file2 ...
//     or  wordfreq -n #  
// (1) reads all files, parses into words separated by whitespace 
//     or generates Eric Goodman's input of (2**(N+1)-1) words (-n option).
// (2) counts occurrence of each word in all files
// (3) prints top 10 words
//
// Based on wordfreq.cpp in the example directory.  Modified for experiments
// with out-of-core MapReduce.

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
void genwords_fileequiv(int, KeyValue *, void *);
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

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  if (me == 0) {
    for (int i=0; i < narg; i++) printf("%s ", args[i]);
    printf("\n");
    fflush(stdout);
  }

  if (narg <= 1) {
    if (me == 0) printf("Syntax: wordfreq file1 file2 ...\nor wordfreq -n #\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  int N = -1;
  bool readfiles = true;
  if (strcmp(args[1], "-n") == 0) {
    readfiles = false;
    N = atoi(args[2]);
  }

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
#ifdef NEW_OUT_OF_CORE
  mr->memsize=1024;
#endif

  if (me == 0) {printf("Beginning map...\n"); fflush(stdout);}
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  int nwords;
  if (readfiles)
    nwords = mr->map(narg-1,&fileread,&args[1]);
  else {
#ifdef FILEEQUIV
    // Automatically generate the words using Eric Goodman's scheme.
    // This version has identical initial distribuion as the file-based input.
    nwords = mr->map(N+1, &genwords_fileequiv, &N);
#else
    // Automatically generate the words using Eric Goodman's scheme.
    // This version has identical initial distribuion as the file-based input.
    // Then redistribute the words equally among the processors.  They will
    // no longer be grouped by word, but they will be balanced on the procs.
    nwords = mr->map(N+1, &genwords_thenbalance, &N);
    if (me == 0) {printf("Beginning balance...\n"); fflush(stdout);}
    mr->collate(&identity);  // Collates by processor
    mr->reduce(&balance, NULL);
#endif

    // This version has better spread of initial data across all procs when,
    // e.g., the number of processors is much larger than the number of files.
    // Funny, I thought this method would be better than the fileequiv
    // method, but as the number of processors increases, it is actually
    // worse (in terms of execution time).  I haven't investigated whether
    // the problem is load-imbalance or more communication (or something
    // else), though.  But for in-core MR-MPI with N=22 on 128 processors,
    // the fileequiv method took ~3.2 seconds, while the wordpertask method
    // takes ~14 seconds.
    // int nuniqueword = (1<<(N+1))-1; 
    // nwords = mr->map(nuniqueword,&genwords_wordpertask,&N);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  double tread = MPI_Wtime();

  if (me == 0) {printf("Beginning reduce...\n"); fflush(stdout);}

#ifdef LOCALCOMPRESS
  // Perform a local compression of the data first to reduce the amount
  // of communication that needs to be done.
  // This compression builds local hash tables, and requires an additional
  // pass over the data..
  mr->compress(&sum,NULL);  // Do word-count locally first
  mr->collate(NULL);        // Hash keys and local counts to processors
  int nunique = mr->reduce(&globalsum,NULL); // Compute global sums for each key
#else
  // Do not do a local compression but, rather, do a global aggregate only.
  mr->collate(NULL);    
  int nunique = mr->reduce(&sum,NULL);
#endif

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

#ifdef POSTPROCESS
  if (me == 0) {printf("Beginning post-processing...\n"); fflush(stdout);}
  mr->sort_values(&ncompare);

  Count count;
  count.n = 0;
  count.limit = 10;
  count.flag = 0;
#ifdef NEW_OUT_OF_CORE
  mr->map(mr, &output, &count);
#else
  mr->map(mr->kv,&output,&count);
#endif
  
  mr->gather(1);
  mr->sort_values(&ncompare);

  count.n = 0;
  count.limit = 10;
  count.flag = 1;
#ifdef NEW_OUT_OF_CORE
  mr->map(mr, &output,&count);
#else
  mr->map(mr->kv,&output,&count);
#endif

#endif // POSTPROCESS

  delete mr;

  MPI_Barrier(MPI_COMM_WORLD);
  double tpost = MPI_Wtime();

  if (me == 0) {
    printf("%d total words, %d unique words\n",nwords,nunique);
    if (readfiles)
      printf("Time for fileread:  %g (secs)\n", tread-tstart);
    else 
      printf("Time for genwords:  %g (secs)\n", tread-tstart);
    printf("Time for wordcount: %g (secs)\n", tstop-tread);
    if (readfiles)
      printf("Total Time to process %d files on %d procs = %g (secs)\n",
	     narg-1,nprocs,tstop-tstart);
    else
      printf("Total Time to process with N=%d on %d procs = %g (secs)\n",
	     N,nprocs,tstop-tstart);
    printf("Time for post-processing:  %g (secs)\n", tpost-tstop);
  }

  MPI_Finalize();
}

/* ----------------------------------------------------------------------
   read a file
   for each word in file, emit key = word, value = NULL
------------------------------------------------------------------------- */

void fileread(int itask, KeyValue *kv, void *ptr)
{
  // filesize = # of bytes in file

  char **files = (char **) ptr;

  struct stat stbuf;
  int flag = stat(files[itask],&stbuf);
  if (flag < 0) {
    printf("ERROR: Could not query file size\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
  int filesize = stbuf.st_size;

  FILE *fp = fopen(files[itask],"r");
  char *text = new char[filesize+1];
  int nchar = fread(text,1,filesize,fp);
  text[nchar] = '\0';
  fclose(fp);

  const char *whitespace = " \t\n\f\r\0";
  char *word = strtok(text,whitespace);
  while (word) {
    kv->add(word,strlen(word)+1,NULL,0);
    word = strtok(NULL,whitespace);
  }

  delete [] text;
}

/* ----------------------------------------------------------------------
   generate words using Eric Goodman's strategy.
   Words are just number strings.
   Generate 2**N 0s; 2**(N-1) 1s and 2s; 2**(N-2) 3s, 4s, 5s and 6s;
            2**(N-3) 7s, 8s, 9s, 10s, 11s, 12s, 13s, and 14s; etc.
   Maxword string is 2**(N+1)-1-1.
   The initial distribution of words is identical to having read
   Eric's files; the number of tasks == the number of files.
   Emit a processor ID as key, so that can redistribute words evenly
   by processor ID.
   Emit the word as the value.
------------------------------------------------------------------------- */

void genwords_thenbalance(int itask, KeyValue *kv, void *ptr)
{
  // itask is the word to be emitted.
  // Compute number of instances of the word to emit.
  static int count = 0;
  static int np;
  if (!count) MPI_Comm_size(MPI_COMM_WORLD, &np);

  int N = *((int *) ptr);

  // Compute word range for this task.
  uint64_t maxword = (1<<(itask+1))-1;
  uint64_t minword = (1<<(itask))-1;
  uint64_t ncopies = (1<<(N-itask));

  for (uint64_t w = minword; w < maxword; w++) {
    char key[32];
    sprintf(key, "%ld\0", w);
    for (uint64_t i = 0; i < ncopies; i++) {
      int proc = count % np;
      count++;
      kv->add((char *) &proc, sizeof(int), key, strlen(key)+1);
    }
  }
}

int identity(char *key, int keybytes)
{
  int p = *((int *) key);
  return p;
}


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
   generate words using Eric Goodman's strategy.
   Words are just number strings.
   Generate 2**N 0s; 2**(N-1) 1s and 2s; 2**(N-2) 3s, 4s, 5s and 6s;
            2**(N-3) 7s, 8s, 9s, 10s, 11s, 12s, 13s, and 14s; etc.
   Maxword string is 2**(N+1)-1-1.
   The initial distribution of words is identical to having read
   Eric's files; the number of tasks == the number of files.
------------------------------------------------------------------------- */

void genwords_fileequiv(int itask, KeyValue *kv, void *ptr)
{
  // itask is the word to be emitted.
  // Compute number of instances of the word to emit.
  int N = *((int *) ptr);

  // Compute word range for this task.
  uint64_t maxword = (1<<(itask+1))-1;
  uint64_t minword = (1<<(itask))-1;
  uint64_t ncopies = (1<<(N-itask));

  for (uint64_t w = minword; w < maxword; w++) {
    char key[32];
    sprintf(key, "%ld\0", w);
    for (uint64_t i = 0; i < ncopies; i++) {
      kv->add(key, strlen(key)+1, NULL, 0);
    }
  }
}
/* ----------------------------------------------------------------------
   generate words using Eric Goodman's strategy.
   Words are just number strings.
   Generate 2**N 0s; 2**(N-1) 1s and 2s; 2**(N-2) 3s, 4s, 5s and 6s;
            2**(N-3) 7s, 8s, 9s, 10s, 11s, 12s, 13s, and 14s; etc.
   Maxword string is 2**(N+1)-1-1.
   All instances of one word are generated per map task.
------------------------------------------------------------------------- */

void genwords_wordpertask(int itask, KeyValue *kv, void *ptr)
{
  // itask is the word to be emitted.
  // Compute number of instances of the word to emit.
  int N = *((int *) ptr);
  int m;

  for (m = N; m >= 0; m--) 
    if ((itask < (1<<(m+1))-1) && (itask >= (1<<m)-1)) break;

  // Emit 2**(N-m) copies of itask.
  char key[32];
  sprintf(key, "%d\0", itask);
  uint64_t ncopies = (1<<(N-m));
  for (uint64_t i = 0; i < ncopies; i++) {
    kv->add(key, strlen(key)+1, NULL, 0);
  }
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

/* ----------------------------------------------------------------------
   compare two counts
   order values by count, largest first
------------------------------------------------------------------------- */

int ncompare(char *p1, int len1, char *p2, int len2)
{
  int i1 = *(int *) p1;
  int i2 = *(int *) p2;
  if (i1 > i2) return -1;
  else if (i1 < i2) return 1;
  else return 0;
}

/* ----------------------------------------------------------------------
   process a word and its count
   depending on flag, emit KV or print it, up to limit
------------------------------------------------------------------------- */

void output(int itask, char *key, int keybytes, char *value,
	    int valuebytes, KeyValue *kv, void *ptr)
{
  Count *count = (Count *) ptr;
  count->n++;
  if (count->n > count->limit) return;

  int n = *(int *) value;
  if (count->flag) printf("%d %s\n",n,key);
  else kv->add(key,keybytes,(char *) &n,sizeof(int));
}