/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

/* MapReduce word frequency example in C
  Syntax: cwordfreq file1 file2 ...
  (1) reads all files, parses into words separated by whitespace
  (2) counts occurrence of each word in all files
  (3) writes word counts to tmp.out */

#include "mpi.h"
#include "stdio.h"
#include "string.h"
#include "cmapreduce.h"

void fileread(int, void *, void *);
void sum(char *, int, char **, void *, void *);
int ncompare(char *, char *);
void output(char *, int, char **, void *, void *);

#define FILESIZE 1000000

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  int me,nprocs;
  int nwords,nunique;
  double tstart,tstop;
  FILE *fp;

  MPI_Init(&narg,&args);
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  if (narg <= 1) {
    if (me == 0) printf("Syntax: cwordfreq file1 file2 ...\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  tstart = MPI_Wtime();

  void *mr = MR_create(MPI_COMM_WORLD);
  MR_set_verbosity(mr,2);

  nwords = MR_map(mr,narg-1,&fileread,&args[1]);
  MR_collate(mr,NULL);
  nunique = MR_reduce(mr,&sum,NULL);
  MR_gather(mr,1);
  MR_sort_values(mr,&ncompare);
  MR_clone(mr);

  fp = NULL;
  if (me == 0) {
    fp = fopen("tmp.out","w");
    fprintf(fp,"# %d unique words\n",nunique);
  }
  MR_reduce(mr,&output,fp);
  if (me == 0) fclose(fp);

  MR_destroy(mr);

  MPI_Barrier(MPI_COMM_WORLD);
  tstop = MPI_Wtime();

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

void fileread(int itask, void *kv, void *ptr)
{
  char *whitespace = " \t\n\f\r\0";
  char text[FILESIZE];

  char **files = (char **) ptr;
  FILE *fp = fopen(files[itask],"r");
  int nchar = fread(text,1,FILESIZE,fp);
  text[nchar] = '\0';
  fclose(fp);

  char *word = strtok(text,whitespace);
  while (word) {
    MR_kv_add(kv,word,strlen(word)+1,NULL,0);
    word = strtok(NULL,whitespace);
  }
}

/* ----------------------------------------------------------------------
   sum reduce() function
   emit key = word, value = # of multi-values
------------------------------------------------------------------------- */

void sum(char *key, int nvalues, char **values, void *kv, void *ptr) 
{
  MR_kv_add(kv,key,strlen(key)+1,(char *) &nvalues,4);
}

/* ----------------------------------------------------------------------
   ncompare compare() function
   order values by count, largest first
------------------------------------------------------------------------- */

int ncompare(char *p1, char *p2)
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

void output(char *key, int nvalues, char **values, void *kv, void *ptr)
{
  FILE *fp = (FILE *) ptr;
  int n = *(int *) values[0];
  fprintf(fp,"%d %s\n",n,key);
}
