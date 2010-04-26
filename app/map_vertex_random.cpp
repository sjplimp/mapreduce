#include "math.h"
#include "stdlib.h"
#include "map_vertex_random.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapVertexRandom::
MapVertexRandom(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg != 1) error->all("Illegal map vertex_random command");

  int seed = atoi(arg[0]);

  int me;
  MPI_Comm_rank(world,&me);
  srand48(seed+me);

  appmap_mr = map;
}

/* ---------------------------------------------------------------------- */

void MapVertexRandom::map(uint64_t itask, char *key, int keybytes, 
			  char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  VPAIR *vpair = (VPAIR *) key;
  EDGE edge;

  edge.vi = vpair->vi;
  edge.ri = vpair->vi;
  edge.vj = vpair->vj;
  edge.rj = vpair->vj;
  kv->add((char *) &edge,sizeof(EDGE),NULL,0);
}
