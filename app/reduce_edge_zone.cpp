#include "reduce_edge_zone.h"
#include "blockmacros.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceEdgeZone::ReduceEdgeZone(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal reduce edge_zone command");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */

void ReduceEdgeZone::reduce(char *key, int keybytes,
			    char *multivalue, int nvalues, int *valuebytes,
			    KeyValue *kv, void *ptr)
{
  int i;
  char *value;

  // loop over values to find zone ID

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  value = multivalue;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] == sizeof(uint64_t)) break;
    value += valuebytes[i];
  }
  if (i < nvalues) break;

  END_BLOCK_LOOP

  uint64_t zone = *(uint64_t *) value;

  // emit one KV per edge with zone ID as value

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  value = multivalue;
  for (int i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(uint64_t))
      kv->add(value,valuebytes[i],(char *) &zone,sizeof(uint64_t));
    value += valuebytes[i];
  }

  END_BLOCK_LOOP
}
