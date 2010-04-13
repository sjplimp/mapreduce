#include "reduce_zone_winner.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

#define INT64MAX 0x7FFFFFFFFFFFFFFF

/* ---------------------------------------------------------------------- */

ReduceZoneWinner:: 
ReduceZoneWinner(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal zone_winner args");

  appreduce = reduce;
  appptr = (void *) this;
}

/* ---------------------------------------------------------------------- */

void ReduceZoneWinner::reduce(char *key, int keybytes,
			      char *multivalue, int nvalues, int *valuebytes,
			      KeyValue *kv, void *ptr)
{
  // z0,z1 have hi-bit stripped off

  uint64_t *z = (uint64_t *) multivalue;
  uint64_t z0 = z[0] & INT64MAX;
  uint64_t z1 = z[1] & INT64MAX;

  if (z0 == z1) return;

  // emit zone pair with hi-bits
  // append extra word to value,
  // so zone can be distinguished from vertex values in next stage of CC

  ReduceZoneWinner *data = (ReduceZoneWinner *) ptr;
  data->flag = 1;
  PAD *pad = &(data->pad);

  if (z0 > z1) {
    pad->zone = z[1];
    kv->add((char *) &z[0],sizeof(uint64_t),(char *) pad,sizeof(PAD));
  } else {
    pad->zone = z[0];
    kv->add((char *) &z[1],sizeof(uint64_t),(char *) pad,sizeof(PAD));
  }
}
