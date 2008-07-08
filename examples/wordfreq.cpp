/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

// MapReduce word frequency example in C++
// Syntax: wordfreq file1 file2 ...
// (1) reads all files, parses into words separated by whitespace
// (2) counts occurrence of each word in all files
// (3) writes word counts to tmp.out

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"

using namespace MAPREDUCE_NS;

void fileread(int, KeyValue *, void *);
void strread(int, char *, int, KeyValue *, void *);
void sum(char *, int, char *, int, int *, KeyValue *, void *);
int ncompare(char *, int, char *, int);
void output(char *, int, char *, int, int *, KeyValue *, void *);

#define FILESIZE 1000000

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  if (narg <= 1) {
    if (me == 0) printf("Syntax: wordfreq file1 file2 ...\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 2;

  //int nwords = mr->map(narg-1,&fileread,&args[1]);
  int nwords = mr->map(100,narg-1,&args[1],'\n',128,&strread,NULL);

  mr->collate(NULL);
  int nunique = mr->reduce(&sum,NULL);
  mr->gather(1);
  mr->sort_values(&ncompare);
  mr->clone();

  FILE *fp = NULL;
  if (me == 0) {
    fp = fopen("tmp.out","w");
    fprintf(fp,"# %d unique words\n",nunique);
  }
  mr->reduce(&output,fp);
  if (me == 0) fclose(fp);

  delete mr;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0) {
    printf("%d total words, %d unique words\n",nwords,nunique);
    printf("Time to wordcount %d files on %d procs = %g (secs)\n",
	   narg-1,nprocs,tstop-tstart);
  }

  MPI_Finalize();
}

/* ----------------------------------------------------------------------
   fileread map() function
   for each word in file, emit key = word, value = NULL
------------------------------------------------------------------------- */

void fileread(int itask, KeyValue *kv, void *ptr)
{
  char *whitespace = " \t\n\f\r";
  char *text = new char[FILESIZE];

  char **files = (char **) ptr;
  FILE *fp = fopen(files[itask],"r");
  int nchar = fread(text,1,FILESIZE,fp);
  text[nchar] = '\0';
  fclose(fp);

  char *word = strtok(text,whitespace);
  while (word) {
    kv->add(word,strlen(word)+1,NULL,0);
    word = strtok(NULL,whitespace);
  }

  delete [] text;
}

/* ----------------------------------------------------------------------
   strread map() function
   for each word in string, emit key = word, value = NULL
------------------------------------------------------------------------- */

void strread(int itask, char *str, int size, KeyValue *kv, void *ptr)
{
  char *whitespace = " \t\n\f\r";
  char *word = strtok(str,whitespace);
  while (word) {
    kv->add(word,strlen(word)+1,NULL,0);
    word = strtok(NULL,whitespace);
  }
}

/* ----------------------------------------------------------------------
   sum reduce() function
   emit key = word, value = # of multi-values
------------------------------------------------------------------------- */

void sum(char *key, int keybytes, char *multivalue,
	 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,(char *) &nvalues,sizeof(int));
}

/* ----------------------------------------------------------------------
   ncompare compare() function
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
   output reduce() function
   print count, word to file
------------------------------------------------------------------------- */

void output(char *key, int keybytes, char *multivalue,
	    int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  FILE *fp = (FILE *) ptr;
  int n = *(int *) multivalue;
  fprintf(fp,"%d %s\n",n,key);
}
