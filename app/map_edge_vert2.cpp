#include "map_edge_vert2.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapEdgeVert2::MapEdgeVert2(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg) error->all("Invalid map edge_vert2 args");

  appmap_mr = map;
}

/* ---------------------------------------------------------------------- */

void MapEdgeVert2::map(uint64_t itask, char *key, int keybytes, 
		       char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  EDGE *edge = (EDGE *) key;
  kv->add((char *) &edge->vi,sizeof(VERTEX),(char *) &edge->vj,sizeof(VERTEX));
  kv->add((char *) &edge->vj,sizeof(VERTEX),(char *) &edge->vi,sizeof(VERTEX));
}
