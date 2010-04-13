#include "stdlib.h"
#include "string.h"
#include "stdio.h"
#include "map_vertex_self.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapVertexSelf::MapVertexSelf(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg != 1) error->all("Invalid map vertex_self args");

  n = atol(arg[0]);

  MPI_Comm_rank(world,&me);
  MPI_Comm_size(world,&nprocs);

  appmap = map;
  appptr = this;
}

/* ---------------------------------------------------------------------- */

void MapVertexSelf::map(int itask, KeyValue *kv, void *ptr)
{
  MapVertexSelf *data = (MapVertexSelf *) ptr;
  uint64_t n = data->n;

  uint64_t nstart = (data->me)*n/(data->nprocs);
  uint64_t nstop = ((data->me)+1)*n/(data->nprocs);

  for (uint64_t i = nstart; i < nstop; i++)
    kv->add((char *) &i,sizeof(uint64_t),(char *) &i,sizeof(uint64_t));
}
