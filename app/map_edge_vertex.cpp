#include "map_edge_vertex.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapEdgeVertex::MapEdgeVertex(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg) error->all("Illegal map edge_vertex command");

  appmap_mr = map;
}

/* ---------------------------------------------------------------------- */

void MapEdgeVertex::map(uint64_t itask, char *key, int keybytes, 
			char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  EDGE *edge = (EDGE *) key;
  kv->add((char *) &edge->vi,sizeof(VERTEX),key,sizeof(EDGE));
  kv->add((char *) &edge->vj,sizeof(VERTEX),key,sizeof(EDGE));
}
