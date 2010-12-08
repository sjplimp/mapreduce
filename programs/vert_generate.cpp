/* ----------------------------------------------------------------------
   generate sequential list of vertices
   nvert = # of vertices
   start = starting index, typically 0 or 1
   input MR = empty
   aggregate vertices to owning processor
   output MR = Vi : NULL
   datatypes: Vi = uint64
 ------------------------------------------------------------------------- */

#include "mpi.h"
#include "vert_generate.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

VertGenerate::VertGenerate(uint64_t nvert_in, int start_in, MPI_Comm world)
{
  nvert = nvert_in;
  start = start_in;

  MPI_Comm_rank(world,&me);
  MPI_Comm_size(world,&nprocs);
}

/* ---------------------------------------------------------------------- */

void VertGenerate::run(MapReduce *mr)
{
  mr->map(nprocs,map,this);
  mr->aggregate(NULL);
}

/* ---------------------------------------------------------------------- */

void VertGenerate::map(int itask, KeyValue *kv, void *ptr)
{
  VertGenerate *data = (VertGenerate *) ptr;

  uint64_t nvert = data->nvert;
  int start = data->start;
  int me = data->me;
  int nprocs = data->nprocs;
  
  if (nvert == 0) return;
  uint64_t nfirst = me*nvert/nprocs;
  uint64_t nlast = (me+1)*nvert/nprocs - 1;
  nfirst += start;
  nlast += start;

  for (uint64_t i = nfirst; i <= nlast; i++) {
    VERTEX vi = i;
    kv->add((char *) &vi,sizeof(VERTEX),NULL,0);
  }
}
