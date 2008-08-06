// MapReduce Matrix Class
// Karen Devine, 1416
// June 2008
//
// The dimensions of the matrix A are given by N and M (N rows, M columns).
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
#include "mrvector.h"

using namespace MAPREDUCE_NS;
using namespace std;


/////////////////////////////////////////////////////////////////////////////
// Vector constructor.  Allocates memory and stores in persistent memory.
// Initializes vector uniformly to 1/n.
MRVector::MRVector(
  MapReduce *mr,
  int n           // Total number of vector entries 
)
{
  if (n == 0) {
    cout << "Invalid vector length 0" << endl;
    exit(-1);
  }

  int me = mr->my_proc();
  int np = mr->num_procs();

  // Distribute the vector evenly among processors.
  int ndivnp = n / np;
  int nmodnp = n % np;
  global_len = n;
  local_len = ndivnp + (me < nmodnp);
  first = me * ndivnp + MIN(me, nmodnp) + 1;

  // Allocated and initialize to uniform values.
  vec = new double[local_len];
  double val = 1. / (double) n;
  for (int i  = 0; i < local_len; i++) vec[i] = val;
}
