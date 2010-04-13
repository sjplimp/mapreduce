#include "stdint.h"
#include "reduce_zone_reassign.h"
#include "blockmacros.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceZoneReassign::
ReduceZoneReassign(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg != 0) error->all("Illegal zone_reassign args");

  //thresh = atoi(arg[0]);

  // set lobits based on nprocs in hi bits of uint64

  appreduce = reduce;
  appptr = (void *) this;
}

/* ---------------------------------------------------------------------- */

void ReduceZoneReassign::reduce(char *key, int keybytes,
				char *multivalue, int nvalues, int *valuebytes,
				KeyValue *kv, void *ptr)
{
  // loop over values, compute winning zone ID

  int i;
  char *value;
  uint64_t znew;
  uint64_t zone = *(uint64_t *) key;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  value = multivalue;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(uint64_t)) {
      znew = *(uint64_t *) value;
      if (znew < zone) zone = znew;
    }
    value += valuebytes[i];
  }

  END_BLOCK_LOOP

  // emit one KV per vertex with zone ID as value

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  value = multivalue;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] == sizeof(uint64_t))
      kv->add(value,valuebytes[i],(char *) &zone,sizeof(uint64_t));
    value += valuebytes[i];
  }

  END_BLOCK_LOOP
}
