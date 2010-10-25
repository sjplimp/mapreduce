// Benchmark to identify CCs in an RMAT matrix
// Syntax: cc_driver -n 10 -e 8 -abcd 0.25 0.25 0.25 0.25 -f 0.8
//                   -s 587283 -t 100
//         -n = order of matrix = 2^n, no default
//         -e = # of edges per row, no default
//         -abcd = RMAT a,b,c,d params, default = 0.25 for each
//         -f = 0.0 <= fraction < 1.0, default = 0.0
//         -s = random # seed = positive int, default = 12345
//         -t = threshhold size of # of vertices in a zone before splitting,
//              default = 0

#include "mpi.h"
#include "stdio.h"
#include "stdint.h"
#include "string.h"
#include "stdlib.h"
#include "rmat_generate.h"
#include "cc_find.h"
#include "cc_stats.h"
#include "mapreduce.h"

#include "localdisks.hpp"

using MAPREDUCE_NS::MapReduce;

struct Params {
  uint64_t nvert,nedge;
  double a,b,c,d;
  double fraction;
  int seed;
  int nthresh;
};

void parse(int narg, char **args, Params *);

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  greetings();
  if (nprocs < 10)  {
    greetings();  // Odin skips some early program output; we'll output extra
    greetings();  // Odin skips some early program output; we'll output extra
    greetings();  // Odin skips some early program output; we'll output extra
  }
  // parse command-line args
  
  Params in;
  parse(narg,args,&in);

  // MRE for matrix edges

  MapReduce *mre = new MapReduce(MPI_COMM_WORLD);
  mre->verbosity = 0;
  mre->timer = 0;

  // generate RMAT matrix

  int niterate;
  RMATGenerate rmat(in.nvert,in.nedge,in.a,in.b,in.c,in.d,in.fraction,in.seed);
  rmat.run(mre,niterate);

  // MRV and MRZ for CC algorithm

  MapReduce *mrv = new MapReduce(MPI_COMM_WORLD);
  mrv->verbosity = 0;
  mrv->timer = 0;
  MapReduce *mrz = new MapReduce(MPI_COMM_WORLD);
  mrz->verbosity = 0;
  mrz->timer = 0;

  // find CCs

  niterate = 0;
  CCFind cc(in.nvert,in.nthresh);
  double time = cc.run(mre,mrv,mrz,niterate);

  if (me == 0)
    printf("CC find: %g secs, %d iterations on RMAT with "
	   "%u verts, %u edges on %d procs\n",time,niterate,
	   in.nvert,in.nedge,nprocs);

  // CC stats

  CCStats stat;
  stat.run(mrv);

  delete mre;
  delete mrv;
  delete mrz;
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
  in->nthresh = 0;

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
    } else if (strcmp(args[iarg],"-t") == 0) {
      if (iarg+2 > narg) break;
      in->nthresh = atoi(args[iarg+1]);
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
