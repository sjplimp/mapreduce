#include "stdlib.h"
#include "stdint.h"
#include "reduce_zone_reassign2.h"
#include "blockmacros.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

#define HIBIT 0x8000000000000000
#define INT64MAX 0x7FFFFFFFFFFFFFFF
#define ALLBITS 0xFFFFFFFFFFFFFFFF

/* ---------------------------------------------------------------------- */

ReduceZoneReassign2::
ReduceZoneReassign2(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg != 1) error->all("Illegal zone_reassign args");

  thresh = atoi(arg[0]);

  int nprocs;
  MPI_Comm_size(world,&nprocs);

  // lmask will mask off hi-bit and proc ID setting in hi-bits of zone

  int pbits = 0;
  while ((1 << pbits) < nprocs) pbits++;
  int hbits = pbits + 1;
  lmask = ALLBITS >> hbits;

  appreduce = reduce;
  appptr = this;
}

/* ---------------------------------------------------------------------- */

void ReduceZoneReassign2::reduce(char *key, int keybytes,
				 char *multivalue, int nvalues,
				 int *valuebytes, KeyValue *kv, void *ptr)
{
  ReduceZoneReassign2 *data = (ReduceZoneReassign2 *) ptr;
  int thresh = data->thresh;
  uint64_t lmask = data->lmask;
  
  int i,hinew;
  char *value;
  uint64_t znew;

  // loop over values, compute winning zone ID
  // hibit is set if winning Z has its hibit set

  uint64_t zcount = 0;
  uint64_t zone = *(uint64_t *) key;
  int hibit = zone >> 63;
  zone &= lmask;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  value = multivalue;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(uint64_t)) {
      znew = *(uint64_t *) value;
      hinew = znew >> 63;
      znew &= INT64MAX;
      if (znew < zone) {
	zone = znew;
	hibit = hinew;
      }
      zcount++;
    }
    value += valuebytes[i];
  }

  END_BLOCK_LOOP

  // emit one KV per vertex with zone ID as value
  // add hi-bit to zone if necessary

  if (hibit || nvalues_total-zcount > thresh) zone |= HIBIT;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  value = multivalue;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] == sizeof(uint64_t))
      kv->add(value,valuebytes[i],(char *) &zone,sizeof(uint64_t));
    value += valuebytes[i];
  }

  END_BLOCK_LOOP
}
