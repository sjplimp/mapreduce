#include "math.h"
#include "stdlib.h"
#include "string.h"
#include "map_rmat_edge.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapRmatEdge::MapRmatEdge(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg != 8) error->all("Illegal map rmat_edge command");

  nlevels = atoi(arg[0]);
  ntotal = atol(arg[1]);
  a = atof(arg[2]);
  b = atof(arg[3]);
  c = atof(arg[4]);
  d = atof(arg[5]);
  fraction = atof(arg[6]);
  int seed = atoi(arg[7]);

  if (a + b + c + d != 1.0) error->all("Rmat_edge a,b,c,d must sum to 1\n");
  if (fraction < 0.0 || fraction >= 1.0)
    error->all("Rmat edge fraction must be < 1\n");

  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);
  srand48(seed+me);

  appmap = map;
  appptr = (void *) this;
}

/* ---------------------------------------------------------------------- */

void MapRmatEdge::map(int itask, KeyValue *kv, void *ptr)
{
  MapRmatEdge *data = (MapRmatEdge *) ptr;

  int nlevels = data->nlevels;
  uint64_t ntotal = data->ntotal;
  double a = data->a;
  double b = data->b;
  double c = data->c;
  double d = data->d;
  double fraction = data->fraction;
  int me = data->me;
  int nprocs = data->nprocs;

  uint64_t order = 1 << (uint64_t) nlevels;
  uint64_t ngenerate = ntotal/nprocs;
  if (me < ntotal % nprocs) ngenerate++;

  uint64_t i,j,delta;
  int ilevel;
  double a1,b1,c1,d1,total,rn;
  EDGE edge;

  for (int m = 0; m < ngenerate; m++) {
    delta = order >> 1;
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
