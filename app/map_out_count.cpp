#include "stdlib.h"
#include "string.h"
#include "stdio.h"
#include "map_out_count.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapOutCount::MapOutCount(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg != 3) error->all("Invalid map out_count args");

  n = atoi(arg[0]);
  limit = atoi(arg[1]);
  flag = atoi(arg[2]);

  appmap_mr = map;
  appptr = this;
}

/* ---------------------------------------------------------------------- */

void MapOutCount::map(uint64_t itask, char *key, int keybytes, 
		      char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  MapOutCount *moc = (MapOutCount *) ptr;
  moc->n++;
  if (moc->n > moc->limit) return;

  int n = *(int *) value;
  if (moc->flag) printf("%d %s\n",n,key);
  else kv->add(key,keybytes,(char *) &n,sizeof(int));
}
