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

  appmap = map;
  appptr = this;
}

/* ---------------------------------------------------------------------- */

void MapVertexSelf::map(int itask, KeyValue *kv, void *ptr)
{
  MapVertexSelf *data = (MapVertexSelf *) ptr;
  uint64_t n = data->n;

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  uint64_t nstart = me*n/nprocs;
  uint64_t nstop = (me+1)*n/nprocs;

  for (uint64_t i = nstart; i < nstop; i++)
    kv->add((char *) &i,sizeof(uint64_t),(char *) &i,sizeof(uint64_t));
}
