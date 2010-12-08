/* ----------------------------------------------------------------------
   label each key with random value from lo to hi inclusive
   lo,hi = bounds of value
   seed = RNG seed to create different RN for each processor via drand48()
   input MR = key : value
   output MR = key : label
   datatypes: key = arbitrary, label = int
 ------------------------------------------------------------------------- */

#include "mpi.h"
#include "stdlib.h"
#include "graph_label.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

GraphLabel::GraphLabel(int lo_in, int hi_in, int seed, MPI_Comm world)
{
  lo = lo_in;
  hi = hi_in;

  int me;
  MPI_Comm_rank(world,&me);
  srand48(seed+me);
}

/* ---------------------------------------------------------------------- */

void GraphLabel::run(MapReduce *mr)
{
  mr->map(mr,map,this);
}

/* ---------------------------------------------------------------------- */

void GraphLabel::map(uint64_t itask, char *key, int keybytes, 
		     char *value, int valuebytes, KeyValue *kv, void *ptr)
{
 GraphLabel *data = (GraphLabel *) ptr;

  int lo = data->lo;
  int hi = data->hi;
  LABEL attribute = static_cast<LABEL> (lo + drand48()*(hi-lo+1));
  kv->add(key,keybytes,(char *) &attribute,sizeof(LABEL));
}
