// MatVec via MapReduce
// Karen Devine, 1416
// June 2008
//
// Performs global sum
//
// Syntax: gs
//
// test of global sum functionality

#include <iostream>
#include <list>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "mrmatrix.h"
#include "mrvector.h"
#include "mrall.h"
#include "blockmacros.h"
#include "localdisks.hpp"

using namespace MAPREDUCE_NS;
using namespace std;

////////////////////////////////////////////////////////////////////////////
// Global variables.

////////////////////////////////////////////////////////////////////////////
int main(int narg, char **args)
{
  int me, np;      // Processor rank and number of processors.
  MPI_Init(&narg,&args);

  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&np);

  MPI_Barrier(MPI_COMM_WORLD);

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 0;

  int gsum = MRGlobalSum(mr, me);
  printf("%d KDDKDD gsum=%d\n", me, gsum);

  MPI_Finalize();
}

