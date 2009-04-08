// MapReduce Global Sum operation.
// Karen Devine, 1416
// August 2008
//  This function computes the global sum of an integer over all processors.
//  Since MapReduce does not yet have a broadcast, we cheat here and use
//  MPI_Bcast.
//
// SVN Information:
//  $Author:$
//  $Date:$
//  $Revision:$

#include <iostream>
#include <list>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "mrall.h"

MAPFUNCTION emit_local;
REDUCEFUNCTION scrunch_sum_int;

int MRGlobalSum(
  MapReduce *mr,
  int my_n
)
{
  int me = mr->my_proc();
  int np = mr->num_procs();
  int gsum = 0;

  // Send all local data to one processor.
  mr->map(np, emit_local, (void *)&my_n, 0);

  mr->scrunch(1, (char *) &me, sizeof(me));

  mr->reduce(&scrunch_sum_int, &gsum);

  // Currently, MapReduce does not have a broadcast, so we'll cheat.
  MPI_Bcast(&gsum, 1, MPI_INT, 0, mr->communicator());
  return gsum;
}


////////////////////////////////////////////////////////////////////////////
void emit_local(int itask, KeyValue *kv, void *ptr)
{
  int zero = 100;
  int value = *((int *) ptr);
  kv->add((char *) &zero, sizeof(zero), (char *) &value, sizeof(value));
}

////////////////////////////////////////////////////////////////////////////
void scrunch_sum_int(char *key, int keylen, char *multivalue, int nvalues, int *mvlen,
             KeyValue *kv, void *ptr)
{
  //  Accumulate the sum.
  int *values = (int *) multivalue;
  int sum = 0;
  for (int i = 0; i < nvalues/2; i++)
    sum += values[2*i+1];
  int *gsum = (int *) ptr;
  *gsum = sum;
}

