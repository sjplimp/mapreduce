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
#include <float.h>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "mrvector.h"

using namespace MAPREDUCE_NS;
using namespace std;

MAPFUNCTION initialize_vec;
MAPFUNCTION emit_vector_entries;

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
  global_len = n;

  mr->map(n, &initialize_vec, (void *)this, 0);
}

/////////////////////////////////////////////////////////////////////////////
// initialize_vec map() function
// Creates and initializes a vector; stores it in persistent memory.
// The vector pointer is sent in the ptr argument.
// Vector entries are initialized to zero.
// This map does not emit any key,value pairs; it only creates the 
// vector entries in memory.
// Should store entries on processors according to row hashing scheme.
void initialize_vec(int itask, KeyValue *kv, void *ptr)
{
  MRVector *x = (MRVector *) ptr;
  x->AddEntry(itask+1, 0.);  // Matrix-market is one-based.
}

/////////////////////////////////////////////////////////////////////////////
// Add an entry to a vector.
void MRVector::AddEntry(
  int i,        // Entry index
  double d      // Entry value
)
{
  INTDOUBLE v;
  v.i = i;
  v.d = d;
// printf("    kddkdd AddEntry %d %f\n", i, d);
  vec.push_front(v);
}

/////////////////////////////////////////////////////////////////////////////
// Empty a vector of all entries.
void MRVector::MakeEmpty()
{
  vec.clear();
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::Print
// Print the vector entries; nothing fancy for parallel here.
void MRVector::Print()
{
  list<INTDOUBLE>::iterator v;
  for (v=vec.begin(); v!=vec.end(); v++)
    printf("[%d  %f]\n", (*v).i, (*v).d);
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::PutScalar
// Assign a scalar value to each entry of a vector.
void MRVector::PutScalar(double d)
{
  list<INTDOUBLE>::iterator v;
  for (v=vec.begin(); v!=vec.end(); v++)
    (*v).d = d;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::Scale
// Multiply each vector entry by a scalar value.
void MRVector::Scale(double d)
{
  list<INTDOUBLE>::iterator v;
  for (v=vec.begin(); v!=vec.end(); v++)
    (*v).d *= d;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::AddScalar
// Add a scalar to each vector entry.
void MRVector::AddScalar(double d)
{
  list<INTDOUBLE>::iterator v;
  for (v=vec.begin(); v!=vec.end(); v++)
    (*v).d += d;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::GlobalMin
// Compute min of all vector entries.
// Requires communication
// Cheating by using MPI_Allreduce instead of MapReduce.
double MRVector::GlobalMin(MapReduce *mr)
{
  double min = LocalMin(); 
  double gmin;
  MPI_Allreduce(&min, &gmin, 1, MPI_DOUBLE, MPI_MIN, mr->communicator());
  return gmin;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::GlobalMax
// Compute max of all vector entries.
// Requires communication
// Cheating by using MPI_Allreduce instead of MapReduce.
double MRVector::GlobalMax(MapReduce *mr)
{
  double max = LocalMax(); 
  double gmax;
  MPI_Allreduce(&max, &gmax, 1, MPI_DOUBLE, MPI_MAX, mr->communicator());
  return gmax;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::GlobalSum
// Compute sum of all vector entries.
// Requires communication
// Cheating by using MPI_Allreduce instead of MapReduce.
double MRVector::GlobalSum(MapReduce *mr)
{
  double sum = LocalSum(); 
  double gsum;
  MPI_Allreduce(&sum, &gsum, 1, MPI_DOUBLE, MPI_SUM, mr->communicator());
  return gsum;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::LocalSum
// Compute sum of all local vector entries.
double MRVector::LocalSum()
{
  double sum = 0.;
  list<INTDOUBLE>::iterator v;
  for (v=vec.begin(); v!=vec.end(); v++)
    sum += (*v).d;
  return sum;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::LocalMin
// Compute min of all local vector entries.
double MRVector::LocalMin()
{
  double min = DBL_MAX;
  list<INTDOUBLE>::iterator v;
  for (v=vec.begin(); v!=vec.end(); v++)
    if ((*v).d < min) min = (*v).d;
  return min;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::LocalMax
// Compute max of all local vector entries.
double MRVector::LocalMax()
{
  double max = 0.;
  list<INTDOUBLE>::iterator v;
  for (v=vec.begin(); v!=vec.end(); v++)
    if ((*v).d > max) max = (*v).d;
  return max;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::EmitEntries
// Emit all entries into MapReduce object mr.
// Flag add indicates whether to add the entries (1) 
// or reset mr before adding (0).
void MRVector::EmitEntries(
  MapReduce *mr, 
  int add
) 
{
  mr->map(mr->num_procs(), emit_vector_entries, (void *)this, add);
}

/////////////////////////////////////////////////////////////////////////////
// emit_vector_entries map() function
// For each entry in input MRVector, emit (key,value) = (i,x_i).
void emit_vector_entries(int itask, KeyValue *kv, void *ptr)
{
  // Assume ptr = MRVector.
  MRVector *x = (MRVector *) ptr;

  list<INTDOUBLE>::iterator v;
  for (v = x->vec.begin(); v != x->vec.end(); v++) {
// printf("    kddkdd EmitVec %d %f\n", (*v).i, (*v).d);
    kv->add((char *)&((*v).i), sizeof((*v).i),
            (char *) &((*v).d), sizeof((*v).d));
  }
}



