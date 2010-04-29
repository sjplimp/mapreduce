/* ----------------------------------------------------------------------
   convert matrix to upper triangular with no diagonal elements
   Eij with Vi < Vj is left alone
   Eij with Vi > Vj is flipped to Eji
   Eij with Vi = Vi is deleted
   any duplicate Eij are deleted
   run() inputs:
     mr = one KV per edge = (Eij,NULL)
   run() outputs:
     mr = one KV per edge = (Eij,NULL), with Vi < Vj
     nedge = # of non-zeroes in final mr
     return elapsed time
 ------------------------------------------------------------------------- */

#include "stdint.h"
#include "stdlib.h"
#include "matrix_upper.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

double MatrixUpper::run(MapReduce *mr, uint64_t &nedge)
{
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // eliminate duplicate edges, if both I,J and J,I exist
  // eliminate diagonal self edges, I,I
  // results in ((Vi,Vj),NULL) with all Vi < Vj

  mr->map(mr,map_invert_drop,NULL);
  mr->collate(NULL);
  nedge = mr->reduce(reduce_cull,NULL);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  return tstop-tstart;
}

/* ---------------------------------------------------------------------- */

void MatrixUpper::map_invert_drop(uint64_t itask, char *key, int keybytes, 
				  char *value, int valuebytes, 
				  KeyValue *kv, void *ptr)
{
  EDGE *edge = (EDGE *) key;
  if (edge->vi < edge->vj) kv->add(key,keybytes,NULL,0);
  else if (edge->vj < edge->vi) {
    EDGE edgeflip;
    edgeflip.vi = edge->vj;
    edgeflip.vj = edge->vi;
    kv->add((char *) &edgeflip,sizeof(EDGE),NULL,0);
  }
}

/* ---------------------------------------------------------------------- */

void MatrixUpper::reduce_cull(char *key, int keybytes,
			      char *multivalue, int nvalues,
			      int *valuebytes, KeyValue *kv, void *ptr)
{
  kv->add(key,keybytes,NULL,0);
}
