/* ----------------------------------------------------------------------
   convert edge matrix to upper triangular with no diagonal elements
   input MR = Eij : value, Eij = Vi Vj
   Eij with Vi < Vj is left alone
   Eij with Vi > Vj is flipped to Eji
   Eij with Vi = Vi is deleted
   collate and reduce to remove any Eij duplicates
   output MR = Eij : NULL
   datatypes: Vi = uint64
 ------------------------------------------------------------------------- */

#include "mpi.h"
#include "stdint.h"
#include "stdlib.h"
#include "matrix_upper.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MatrixUpper::MatrixUpper(MPI_Comm world_in)
{
  world = world_in;
}

/* ---------------------------------------------------------------------- */

double MatrixUpper::run(MapReduce *mr, uint64_t &nedge)
{
  MPI_Barrier(world);
  double tstart = MPI_Wtime();

  mr->map(mr,map,NULL);
  mr->collate(NULL);
  nedge = mr->reduce(reduce,NULL);

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  return tstop-tstart;
}

/* ---------------------------------------------------------------------- */

void MatrixUpper::map(uint64_t itask, char *key, int keybytes, 
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

void MatrixUpper::reduce(char *key, int keybytes,
			 char *multivalue, int nvalues,
			 int *valuebytes, KeyValue *kv, void *ptr)
{
  kv->add(key,keybytes,NULL,0);
}
