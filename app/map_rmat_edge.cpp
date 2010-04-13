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
  if (narg != 7) error->all("Illegal map rmat_edge command");

  nrows = atol(arg[0]);
  a = atof(arg[1]);
  b = atof(arg[2]);
  c = atof(arg[3]);
  d = atof(arg[4]);
  fraction = atof(arg[5]);
  int seed = atoi(arg[6]);

  if (a + b + c + d != 1.0) error->all("Rmat_edge a,b,c,d must sum to 1\n");
  if (fraction < 0.0 || fraction >= 1.0)
    error->all("Rmat_edge fraction must be < 1\n");

  // require nrows = power of 2

  nlevels = 0;
  while ((1 << nlevels) < nrows) nlevels++;
  if ((1 << nlevels) != nrows)
    error->all("Rmat_edge nrows must be power of 2");

  MPI_Comm_rank(world,&me);
  MPI_Comm_size(world,&nprocs);
  srand48(seed+me);

  appmap = map;
  appptr = this;
}

/* ---------------------------------------------------------------------- */

void MapRmatEdge::map(int itask, KeyValue *kv, void *ptr)
{
  MapRmatEdge *data = (MapRmatEdge *) ptr;

  uint64_t ngenerate = data->ngenerate;
  uint64_t nrows = data->nrows;
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
    delta = nrows >> 1;
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
