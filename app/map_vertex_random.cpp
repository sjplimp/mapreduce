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

  seed = atoi(arg[0]);

  appmap_mr = map;
  appptr = this;
}

/* ---------------------------------------------------------------------- */

void MapVertexRandom::map(uint64_t itask, char *key, int keybytes, 
			  char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  VPAIR *vpair = (VPAIR *) key;
  EDGE edge;

  MapVertexRandom *data = (MapVertexRandom *) ptr;

  edge.vi = vpair->vi;
  srand48(edge.vi + data->seed);
  edge.ri = drand48();
  edge.vj = vpair->vj;
  srand48(edge.vj + data->seed);
  edge.rj = drand48();
  kv->add((char *) &edge,sizeof(EDGE),NULL,0);
}
