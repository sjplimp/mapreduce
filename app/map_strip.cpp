#include "map_strip.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

#define INT64MAX 0x7FFFFFFFFFFFFFFF

/* ---------------------------------------------------------------------- */

MapStrip::MapStrip(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg) error->all("Illegal map strip command");

  appmap_mr = map;
}

/* ---------------------------------------------------------------------- */

void MapStrip::map(uint64_t itask, char *key, int keybytes, 
		    char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  uint64_t zone = *(uint64_t *) value;
  zone &= INT64MAX;
  kv->add(key,keybytes,(char *) &zone,sizeof(uint64_t));
}
