/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#include "mpi.h"
#include "stdlib.h"
#include "string.h"
#include "rmat.h"
#include "map_rmat_edge.h"
#include "reduce_cull.h"
#include "object.h"
#include "error.h"

#include "mapreduce.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;

enum{MAPREDUCE,MAP,REDUCE,HASH,COMPARE};   // same as in object.cpp

/* ---------------------------------------------------------------------- */

RMAT::RMAT(APP *app) : Pointers(app) {}

/* ---------------------------------------------------------------------- */

void RMAT::command(int narg, char **arg)
{
  if (narg != 9) error->all("Illegal rmat command");

  int me,nprocs;
  MPI_Comm_rank(world,&me);
  MPI_Comm_size(world,&nprocs);

  MapReduce *mr = (MapReduce *) obj->find_object(arg[0],MAPREDUCE);
  if (!mr) error->all("Rmat MRMPI is invalid");

  uint64_t ntotal = atol(arg[1]);
  uint64_t nrows = atol(arg[2]);

  // map and reduce functions

  MapRmatEdge *egen = new MapRmatEdge(app,"egen",narg-2,&arg[2]);
  ReduceCull *cull = new ReduceCull(app,"cull",0,NULL);

  // loop until desired number of unique nonzero entries
  // set egen->ngenerate on each iteration

  MPI_Barrier(world);
  double tstart = MPI_Wtime();

  int niterate = 0;
  uint64_t nremain = ntotal;

  while (nremain) {
    niterate++;
    egen->ngenerate = nremain;
    mr->map(nprocs,egen->appmap,egen->appptr,1);
    uint64_t nunique = mr->collate(NULL);
    mr->reduce(cull->appreduce,cull->appptr);
    nremain = ntotal - nunique;
  }

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  delete egen;
  delete cull;

  if (me == 0) {
    printf("%lu rows in matrix\n",nrows);
    printf("%lu nonzeroes in matrix\n",ntotal);
  }

  if (me == 0)
    printf("%g secs to generate matrix on %d procs in %d iterations\n",
	   tstop-tstart,nprocs,niterate);
}
