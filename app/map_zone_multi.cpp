#include "stdlib.h"
#include "map_zone_multi.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

#define HIBIT 0x8000000000000000
#define INT64MAX 0x7FFFFFFFFFFFFFFF

/* ---------------------------------------------------------------------- */

MapZoneMulti::MapZoneMulti(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg) error->all("Invalid map zone_multi args");

  MPI_Comm_size(world,&nprocs);

  // pshift = amount to left shift a proc ID, to put it 1 bit away from top

  int pbits = 0;
  while ((1 << pbits) < nprocs) pbits++;
  pshift = 63 - pbits;

  appmap_mr = map;
  appptr = this;
}

/* ---------------------------------------------------------------------- */

void MapZoneMulti::map(uint64_t itask, char *key, int keybytes, 
		       char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  uint64_t z = *(uint64_t *) key;

  // if z has hibit set:
  // remove hibit, add random iproc in hibits, reset hibit

  if (z >> 63) {
    MapZoneMulti *data = (MapZoneMulti *) ptr;
    uint64_t zstrip = z & INT64MAX;
    kv->add((char *) &zstrip,sizeof(uint64_t),value,valuebytes);
    int nprocs = data->nprocs;
    int pshift = data->pshift;
    uint64_t znew;
    for (uint64_t iproc = 0; iproc < nprocs; iproc++) {
      znew = zstrip | (iproc << pshift);
      znew |= HIBIT;
      kv->add((char *) &znew,sizeof(uint64_t),value,valuebytes);
    }
  } else kv->add(key,keybytes,value,valuebytes);
}
