/* ----------------------------------------------------------------------
   generate an RMAT matrix via marble-drop algorithm
   Nvert,Nedge = size of matrix to construct
   a,b,c,d,fraction = RMAT params
   seed = random # seed = positive int
   input MR = empty
   output MR = one KV per unique edge = Eij : NULL, Eij = Vi Vj
   Vi are from 0 to Nvert-1
   niterate = # of iterations required
   datatypes: Vi = uint64
 ------------------------------------------------------------------------- */

#include "mpi.h"
#include "math.h"
#include "stdint.h"
#include "stdlib.h"
#include "rmat_generate.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

RMATGenerate::RMATGenerate(uint64_t nvert_in, uint64_t nedge_in,
			   double a_in, double b_in, double c_in, double d_in,
			   double fraction_in, int seed, MPI_Comm world_in)
{
  nvert = nvert_in;
  nedge = nedge_in;
  a = a_in;
  b = b_in;
  c = c_in;
  d = d_in;
  fraction = fraction_in;
  world = world_in;

  MPI_Comm_rank(world,&me);
  MPI_Comm_size(world,&nprocs);

  if (a + b + c + d != 1.0) {
    if (me == 0) printf("ERROR: RMATGenerate a,b,c,d must sum to 1\n");
    MPI_Abort(world,1);
  }
  if (fraction < 0.0 || fraction >= 1.0) {
    if (me == 0) printf("ERROR: RMATGenerate fraction must be < 1\n");
    MPI_Abort(world,1);
  }
  if (seed <= 0) {
    if (me == 0) printf("ERROR: RMATGenerate seed must be positive int\n");
    MPI_Abort(world,1);
  }

  uint64_t one = 1;
  nlevels = 0;
  while ((one << nlevels) < nvert) nlevels++;
  if ((one << nlevels) != nvert) {
    if (me == 0) printf("ERROR: RMATGenerate nvert must be power of 2");
    MPI_Abort(world,1);
  }

  srand48(seed+me);
}

/* ---------------------------------------------------------------------- */

double RMATGenerate::run(MapReduce *mr, int &niterate)
{
  // loop until desired number of unique nonzero entries

  MPI_Barrier(world);
  double tstart = MPI_Wtime();

  niterate = 0;
  ngenerate = nedge;

  while (ngenerate) {
    niterate++;
    mr->map(nprocs,map_rmat_edge,this,1);
    uint64_t nunique = mr->collate(NULL);
    mr->reduce(reduce_cull,NULL);
    ngenerate = nedge - nunique;
  }

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  return tstop-tstart;
}

/* ---------------------------------------------------------------------- */

void RMATGenerate::map_rmat_edge(int itask, KeyValue *kv, void *ptr)
{
  RMATGenerate *data = (RMATGenerate *) ptr;

  uint64_t ngenerate = data->ngenerate;
  uint64_t nvert = data->nvert;
  int nlevels = data->nlevels;
  double a = data->a;
  double b = data->b;
  double c = data->c;
  double d = data->d;
  double fraction = data->fraction;
  int me = data->me;
  int nprocs = data->nprocs;
  
  uint64_t n = ngenerate/nprocs;
  if (me < (ngenerate % nprocs)) n++;
  
  uint64_t i,j,delta;
  int ilevel;
  double a1,b1,c1,d1,total,rn;
  EDGE edge;

  for (uint64_t m = 0; m < n; m++) {
    delta = nvert >> 1;
    a1 = a; b1 = b; c1 = c; d1 = d;
    i = j = 0;

    for (ilevel = 0; ilevel < nlevels; ilevel++) {
      rn = drand48();
      if (rn < a1) {
      } else if (rn < a1+b1) {
	j += delta;
      } else if (rn < a1+b1+c1) {
	i += delta;
      } else {
	i += delta;
	j += delta;
      }
      
      delta /= 2;
      if (fraction > 0.0) {
	a1 += a1*fraction * (drand48() - 0.5);
	b1 += b1*fraction * (drand48() - 0.5);
	c1 += c1*fraction * (drand48() - 0.5);
	d1 += d1*fraction * (drand48() - 0.5);
	total = a1+b1+c1+d1;
	a1 /= total;
	b1 /= total;
	c1 /= total;
	d1 /= total;
      }
    }

    edge.vi = i;
    edge.vj = j;
    kv->add((char *) &edge,sizeof(EDGE),NULL,0);
  }
}

/* ---------------------------------------------------------------------- */

void RMATGenerate::reduce_cull(char *key, int keybytes,
			       char *multivalue, int nvalues, int *valuebytes,
			       KeyValue *kv, void *ptr)
{
  kv->add(key,keybytes,NULL,0);
}
