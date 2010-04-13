#include "map_low_degree.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapLowDegree::MapLowDegree(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg) error->all("Illegal map low_degree command");

  appmap_mr = map;
}

/* ---------------------------------------------------------------------- */
// low-degree vertex emits (vi,vj)
// break tie with low-index vertex

void MapLowDegree::map(uint64_t itask, char *key, int keybytes, 
		       char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  EDGE *edge = (EDGE *) key;
  DEGREE *degree = (DEGREE *) value;

  if (degree->di < degree->dj)
    kv->add((char *) &edge->vi,sizeof(VERTEX),
	    (char *) &edge->vj,sizeof(VERTEX));
  else if (degree->dj < degree->di)
    kv->add((char *) &edge->vj,sizeof(VERTEX),
	    (char *) &edge->vi,sizeof(VERTEX));
  else if (edge->vi < edge->vj)
    kv->add((char *) &edge->vi,sizeof(VERTEX),
	    (char *) &edge->vj,sizeof(VERTEX));
  else
    kv->add((char *) &edge->vj,sizeof(VERTEX),
	    (char *) &edge->vi,sizeof(VERTEX));
}
