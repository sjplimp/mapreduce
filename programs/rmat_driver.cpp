// Benchmark to generate an RMAT matrix
// Syntax: rmat_driver -n 10 -e 8 -abcd 0.25 0.25 0.25 0.25 -f 0.8 -s 587283
//         -n = order of matrix = 2^n, no default
//         -e = # of edges per row, no default
//         -abcd = RMAT a,b,c,d params, default = 0.25 for each
//         -f = 0.0 <= fraction < 1.0, default = 0.0
//         -s = random # seed = positive int, default = 12345

#include "mpi.h"
#include "stdio.h"
#include "stdint.h"
#include "string.h"
#include "stdlib.h"
#include "rmat_generate.h"
#include "matrix_stats.h"
#include "mapreduce.h"

using MAPREDUCE_NS::MapReduce;

struct Params {
  uint64_t nvert,nedge;
  double a,b,c,d;
  double fraction;
  int seed;
};

void parse(int narg, char **args, Params *);

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  // parse command-line args
  
  Params in;
  parse(narg,args,&in);

  // MR for matrix edges

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 1;
  mr->timer = 1;

  // generate RMAT matrix

  int niterate;
  RMATGenerate rmat(in.nvert,in.nedge,in.a,in.b,in.c,in.d,in.fraction,in.seed,
		    MPI_COMM_WORLD);
  double time = rmat.run(mr,niterate);

  if (me == 0)
    printf("RMAT creation: %g secs, %d iterations with "
	   "%u verts, %u edges on %d procs\n",time,niterate,
	   in.nvert,in.nedge,nprocs);

  // RMAT matrix stats

  MatrixStats stat;
  stat.run(mr,in.nvert);

  delete mr;
  MPI_Finalize();
}

/* ---------------------------------------------------------------------- */

void parse(int narg, char **args, Params *in)
{
  int order = -1;
  int nonzero = 0;
  in->a = in->b = in->c = in->d = 0.25;
  in->fraction = 0.0;
  in->seed = 12345;

  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-n") == 0) {
      if (iarg+2 > narg) break;
      order = atoi(args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-e") == 0) {
      if (iarg+2 > narg) break;
      nonzero = atoi(args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-abcd") == 0) {
      if (iarg+5 > narg) break;
      in->a = atof(args[iarg+1]);
      in->b = atof(args[iarg+2]);
      in->c = atof(args[iarg+3]);
      in->d = atof(args[iarg+4]);
      iarg += 5;
    } else if (strcmp(args[iarg],"-f") == 0) {
      if (iarg+2 > narg) break;
      in->fraction = atof(args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-s") == 0) {
      if (iarg+2 > narg) break;
      in->seed = atoi(args[iarg+1]);
      iarg += 2;
    } else break;
  }

  int me;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);

  if (iarg < narg) {
    if (me == 0) printf("ERROR: Invalid command-line args");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
  if (order < 0 || nonzero == 0) {
    if (me == 0) printf("ERROR: No command-line setting for -n or -e");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  uint64_t one = 1;
  in->nvert = one << order;
  in->nedge = in->nvert * nonzero;
}
