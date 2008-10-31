// MatVec via MapReduce
// Karen Devine and Steve Plimpton, Sandia Natl Labs
// Nov 2008
//
// Identify connected components in a graph via MapReduce
// algorithm due to Jonathan Cohen
// 
// Syntax: cc switch args switch args ...
// switches:
//   -r N = define N as root vertex, compute all distances from it
//   -o file = output to this file, else no output except screen summary
//   -t style params = input from a test problem
//      style params = ring N = 1d ring with N vertices
//      style params = 2d Nx Ny = 2d grid with Nx by Ny vertices
//      style params = 3d Nx Ny Nz = 3d grid with Nx by Ny by Nz vertices
//   -f file1 file2 ... = input from list of files containing sparse matrix

#include "mpi.h"
#include "math.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"

using namespace MAPREDUCE_NS;

#define MAXLINE 256

enum{NOINPUT,RING,GRID2D,GRID3D,FILES};

void read_matrix(int, char *, int, KeyValue *, void *);
void ring(int, KeyValue *, void *);
void grid2d(int, KeyValue *, void *);
void grid3d(int, KeyValue *, void *);
void reduce1(char *, int, char *, int, int *, KeyValue *, void *);
void reduce2(char *, int, char *, int, int *, KeyValue *, void *);
void reduce3(char *, int, char *, int, int *, KeyValue *, void *);
void reduce4(char *, int, char *, int, int *, KeyValue *, void *);
int sort(char *, int, char *, int);
void error(int, char *);
void errorone(char *);

struct CC {
  int root;
  int input;
  int nring;
  int nx,ny,nz;
  int nfiles;
  char **infiles;
  char *outfile;
};

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  // parse command-line args

  CC cc;
  cc.root = -1;
  cc.input = NOINPUT;
  cc.nfiles = 0;
  cc.infiles = NULL;
  cc.outfile = NULL;

  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-r") == 0) {
      if (iarg+2 > narg) error(me,"Bad arguments");
      cc.root = atoi(args[iarg+1]);
      iarg += 2;

    } else if (strcmp(args[iarg],"-o") == 0) {
      if (iarg+2 > narg) error(me,"Bad arguments");
      int n = strlen(args[iarg+1]) + 1;
      cc.outfile = new char[n];
      strcpy(cc.outfile,args[iarg+1]);
      iarg += 2;

    } else if (strcmp(args[iarg],"-t") == 0) {
      if (iarg+2 > narg) error(me,"Bad arguments");
      if (strcmp(args[iarg+1],"ring") == 0) {
	if (iarg+3 > narg) error(me,"Bad arguments");
	cc.input = RING;
	cc.nring = atoi(args[iarg+2]); 
	iarg += 3;
      } else if (strcmp(args[iarg+1],"grid2d") == 0) {
	if (iarg+4 > narg) error(me,"Bad arguments");
	cc.input = GRID2D;
	cc.nx = atoi(args[iarg+2]); 
	cc.ny = atoi(args[iarg+3]); 
	iarg += 4;
      } else if (strcmp(args[iarg+1],"grid3d") == 0) {
	if (iarg+5 > narg) error(me,"Bad arguments");
	cc.input = GRID3D;
	cc.nx = atoi(args[iarg+2]); 
	cc.ny = atoi(args[iarg+3]); 
	cc.nz = atoi(args[iarg+4]); 
	iarg += 5;
      } else error(me,"Bad arguments");

    } else if (strcmp(args[iarg],"-f") == 0) {
      cc.input = FILES;
      iarg++;
      while (iarg < narg) {
	if (args[iarg][0] == '-') break;
	cc.infiles = 
	  (char **) realloc(cc.infiles,(cc.nfiles+1)*sizeof(char *));
	cc.infiles[cc.nfiles] = args[iarg];
	cc.nfiles++;
	iarg++;
      }
    } else error(me,"Bad arguments");
  }

  if (cc.input == NOINPUT) error(me,"No input specified");

  // find connected components via MapReduce

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 2;

  if (cc.input == FILES)
    mr->map(nprocs,cc.nfiles,cc.infiles,'\n',80,&read_matrix,&cc);
  else if (cc.input == RING)
    mr->map(nprocs,&ring,&cc);
  else if (cc.input == GRID2D)
    mr->map(nprocs,&grid2d,&cc);
  else if (cc.input == GRID3D)
    mr->map(nprocs,&grid3d,&cc);

  // need to mark root vertex if specified, relabel with ID = 0 ??

  mr->collate(NULL);
  mr->reduce(&reduce1,&cc);

  while (1) {
    mr->collate(NULL);
    mr->reduce(&reduce2,&cc);

    mr->collate(NULL);
    int doneflag = 0;
    mr->reduce(&reduce3,&cc);

    int alldone;
    MPI_Allreduce(&doneflag,&alldone,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
    if (alldone) break;

    mr->collate(NULL);
    mr->reduce(&reduce4,&cc);
  }

  delete mr;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  // statistics
  // test answer if test problem was used
  // do more output if -o flag was specified

  if (me == 0)
    printf("Time to compute CC on %d procs = %g (secs)\n",nprocs,tstop-tstart);

  // clean up

  delete [] cc.outfile;
  free(cc.infiles);

  MPI_Finalize();
}

/* ----------------------------------------------------------------------
   read_matrix function for map
------------------------------------------------------------------------- */

void read_matrix(int itask, char *str, int size, KeyValue *kv, void *ptr)
{
  char *line = strtok(str,"\n");
  while (line) {
    // parse line and emit KVs
    line = strtok(NULL,"\n");
  }
}

/* ----------------------------------------------------------------------
   ring function for map
------------------------------------------------------------------------- */

void ring(int itask, KeyValue *kv, void *ptr)
{
}

/* ----------------------------------------------------------------------
   ring function for map
------------------------------------------------------------------------- */

void grid2d(int itask, KeyValue *kv, void *ptr)
{
}

/* ----------------------------------------------------------------------
   ring function for map
------------------------------------------------------------------------- */

void grid3d(int itask, KeyValue *kv, void *ptr)
{
}

/* ----------------------------------------------------------------------
   reduce1 function
------------------------------------------------------------------------- */

void reduce1(char *key, int keybytes, char *multivalue,
	      int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
}

/* ----------------------------------------------------------------------
   reduce2 function
------------------------------------------------------------------------- */

void reduce2(char *key, int keybytes, char *multivalue,
	      int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
}

/* ----------------------------------------------------------------------
   reduce3 function
------------------------------------------------------------------------- */

void reduce3(char *key, int keybytes, char *multivalue,
	      int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
}

/* ----------------------------------------------------------------------
   reduce4 function
------------------------------------------------------------------------- */

void reduce4(char *key, int keybytes, char *multivalue,
	      int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
}

/* ----------------------------------------------------------------------
   sort function for sorting KMV values
------------------------------------------------------------------------- */

int sort(char *p1, int len1, char *p2, int len2)
{
  int i1 = *(int *) p1;
  int i2 = *(int *) p2;
  if (i1 < i2) return -1;
  else if (i1 > i2) return 1;
  else return 0;
}

/* ---------------------------------------------------------------------- */

void error(int me, char *str)
{
  if (me == 0) printf("ERROR: %s\n",str);
  MPI_Abort(MPI_COMM_WORLD,1);
}

/* ---------------------------------------------------------------------- */

void errorone(char *str)
{
  printf("ERROR: %s\n",str);
  MPI_Abort(MPI_COMM_WORLD,1);
}
