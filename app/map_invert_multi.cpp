#include "stdlib.h"
#include "map_invert_multi.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

#define HIBIT 0x8000000000000000
#define INT64MAX 0x7FFFFFFFFFFFFFFF

/* ---------------------------------------------------------------------- */

MapInvertMulti::MapInvertMulti(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg != 0) error->all("Invalid map invert_multi args");

  int me;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  // hardwire a seed

  int seed = 123456789;
  srand48(seed+me);

  // pshift = amount to left shift a proc ID, to put it 1 bit away from top

  int pbits = 0;
  while ((1 << pbits) < nprocs) pbits++;
  pshift = 63 - pbits;

  appmap_mr = map;
  appptr = (void *) this;
}

/* ---------------------------------------------------------------------- */

void MapInvertMulti::map(uint64_t itask, char *key, int keybytes, 
			 char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  uint64_t z = *(uint64_t *) value;

  // if z has hibit set, add random iproc in hibits, retain hibit setting

  if (z >> 63) {
    MapInvertMulti *data = (MapInvertMulti *) ptr;
    uint64_t iproc = static_cast<uint64_t> (data->nprocs * drand48());
    uint64_t znew = z | (iproc << data->pshift);
    kv->add((char *) &znew,sizeof(uint64_t),key,keybytes);
  } else kv->add(value,valuebytes,key,keybytes);
}
