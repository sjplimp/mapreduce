/* ----------------------------------------------------------------------
   OINK - Mapreduce-MPI library application
   http://www.sandia.gov/~sjplimp/mapreduce.html, Sandia National Laboratories
   Steve Plimpton, sjplimp@sandia.gov

   See the README file in the top-level MR-MPI directory.
------------------------------------------------------------------------- */

#include "mpi.h"
#include "stdio.h"
#include "string.h"
#include "stdlib.h"
#include "rmat.h"
#include "typedefs.h"
#include "object.h"
#include "style_map.h"
#include "style_reduce.h"
#include "style_scan.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace OINK_NS;
using namespace MAPREDUCE_NS;

/* ---------------------------------------------------------------------- */

RMAT::RMAT(OINK *oink) : Command(oink)
{
  ninputs = 0;
  noutputs = 1;
}

/* ---------------------------------------------------------------------- */

void RMAT::run()
{
  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  // mr = matrix edges

  MapReduce *mr = obj->create_mr();

  // loop until desired number of unique nonzero entries

  int niterate = 0;
  int ntotal = (1 << rmat.nlevels) * rmat.nnonzero;
  int nremain = ntotal;
  while (nremain) {
    niterate++;
    rmat.ngenerate = nremain/nprocs;
    if (me < nremain % nprocs) rmat.ngenerate++;
    mr->map(nprocs,rmat_generate,&rmat,1);
    int nunique = mr->collate(NULL);
    mr->reduce(cull,&rmat);
    nremain = ntotal - nunique;
  }

  obj->output(1,mr,print_edge,NULL);

  char msg[128];
  sprintf(msg,"RMAT: %d rows, %d non-zeroes, %d iterations",
	  rmat.order,ntotal,niterate);
  if (me == 0) error->message(msg);

  // stats on number of nonzeroes per row

  if (statflag) {
    if (obj->permanent(mr)) mr = obj->copy_mr(mr);
    mr->reduce(nonzero,NULL);
    mr->collate(NULL);
    mr->reduce(degree,NULL);
    mr->collate(NULL);
    mr->reduce(histo,NULL);
    mr->gather(1);
    mr->sort_keys(1);
    int total = 0;
    mr->map(mr,rmat_stats,&total);
    sprintf(msg,"RMAT: %d rows with 0 non-zeroes",rmat.order-total);
    if (me == 0) error->message(msg);
  }

  obj->cleanup();
}

/* ---------------------------------------------------------------------- */

void RMAT::params(int narg, char **arg)
{
  if (narg != 8) error->all("Illegal rmat command");

  rmat.nlevels = atoi(arg[0]); 
  rmat.nnonzero = atoi(arg[1]); 
  rmat.a = atof(arg[2]); 
  rmat.b = atof(arg[3]); 
  rmat.c = atof(arg[4]); 
  rmat.d = atof(arg[5]); 
  rmat.fraction = atof(arg[6]); 
  int seed = atoi(arg[7]);

  statflag = 1;

  if (rmat.a + rmat.b + rmat.c + rmat.d != 1.0)
    error->all("RMAT a,b,c,d must sum to 1");
  if (rmat.fraction >= 1.0) error->all("RMAT fraction must be < 1");

  int me;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  srand48(seed+me);
  rmat.order = 1 << rmat.nlevels;
}

