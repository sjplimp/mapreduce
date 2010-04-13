#include "map_invert_drop.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapInvertDrop::MapInvertDrop(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg) error->all("Illegal map invert_drop command");

  appmap_mr = map;
}

/* ---------------------------------------------------------------------- */
// invert edges so all have vi < vj
// drop edges where vi = vj

void MapInvertDrop::map(uint64_t itask, char *key, int keybytes, 
			char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  EDGE *edge = (EDGE *) key;
  if (edge->vi < edge->vj) kv->add(key,keybytes,NULL,0);
  else if (edge->vj < edge->vi) {
    EDGE edgeflip;
    edgeflip.vi = edge->vj;
    edgeflip.vj = edge->vi;
    kv->add((char *) &edgeflip,sizeof(EDGE),NULL,0);
  }
}
