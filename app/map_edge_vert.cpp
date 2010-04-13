#include "map_edge_vert.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapEdgeVert::MapEdgeVert(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg) error->all("Illegal map edge_vert command");

  appmap_mr = map;
}

/* ---------------------------------------------------------------------- */

void MapEdgeVert::map(uint64_t itask, char *key, int keybytes, 
		      char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  EDGE *edge = (EDGE *) key;
  kv->add((char *) &edge->vi,sizeof(VERTEX),NULL,0);
}
