#include "map_invert.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapInvert::MapInvert(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg) error->all("Invalid map invert args");

  appmap_mr = map;
}

/* ---------------------------------------------------------------------- */

void MapInvert::map(uint64_t itask, char *key, int keybytes, 
		    char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  kv->add(value,valuebytes,key,keybytes);
}
