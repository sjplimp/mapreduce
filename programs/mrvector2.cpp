// MapReduce Vectorr Class
// Karen Devine, 1416
// April 2010

#include <iostream>
#include <list>
#include <float.h>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "mrvector2.h"

using namespace MAPREDUCE_NS;
using namespace std;

/////////////////////////////////////////////////////////////////////////////
// initialize_vec map() function
// Initializes a vector; emits its values to be hashed to processors by index.
// Vector entries are initialized to zero.
// Output:  Key = global index;  Value = 0.
template <typename IDTYPE>
static void initialize_vec(int itask, KeyValue *kv, void *ptr)
{
  double zero = 0.;
  int nprocs;
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  IDTYPE n = *((IDTYPE *) ptr);
  IDTYPE share = n / nprocs;
  IDTYPE rem = n % nprocs;
  IDTYPE first = itask * share + 1;  // Matrix-market is one-based.
  first += (itask < rem);
  IDTYPE last = first + share + ((itask + 1) < rem);

  for (IDTYPE i = first; i < last; i++)
    kv->add((char *) &i, sizeof(i), (char *)&zero, sizeof(zero));
}

/////////////////////////////////////////////////////////////////////////////
// Vector constructor.  Allocates memory and stores in persistent memory.
// Initializes vector uniformly to 1/n.
template <typename IDTYPE>
MRVector::MRVector(
  IDTYPE n                // Total number of vector entries 
)
{
  if (n == 0) {
    cout << "Invalid vector length 0" << endl;
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  global_len = n;

  mr = new MapReduce(MPI_COMM_WORLD);

  // Emit vector values
  mr->map(nprocs, &initialize_vec, &n);

  // Gather values to processors based on vector index.
  mr->aggregate(NULL);
}

/////////////////////////////////////////////////////////////////////////////
// Empty a vector of all entries.
void MRVector::MakeEmpty()
{
  delete mr;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::Print
// Print the vector entries; nothing fancy for parallel here.
template <typename IDTYPE>
static void printvector(uint64_t itask, char *key, int keybytes,
                        char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  cout << *((IDTYPE *) key) << "\t" << *((double *) value) << endl;
}

void MRVector::Print()
{
  int me;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  cout << "Vector on processor " << me << endl;
  mr->map(mr, &printvector, NULL, 1);  // set addflag to keep mr as is.
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::PutScalar
// Assign a scalar value to each entry of a vector.
static void setvector(uint64_t itask, char *key, int keybytes, 
                      char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double d = *((double *) ptr);
  kv->add(key, keybytes, (char *) &d, sizeof(double));
}

void MRVector::PutScalar(double d)
{
  mr->map(mr, &setvector, &d);
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::Scale
// Multiply each vector entry by a scalar value.
static void scalevector(uint64_t itask, char *key, int keybytes, 
                        char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double d = *((double *) ptr);
  double v = *((double *) value) * d;
  kv->add(key, keybytes, (char *) &v, sizeof(double));
}

void MRVector::Scale(double d)
{
  mr->map(mr, &scalevector, &d);
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::AddScalar
// Add a scalar to each vector entry.
static void addscalarvector(uint64_t itask, char *key, int keybytes, 
                           char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double d = *((double *) ptr);
  double v = *((double *) value) + d;
  kv->add(key, keybytes, (char *) &v, sizeof(double));
}

void MRVector::AddScalar(double d)
{
  mr->map(mr, &addscalarvector, &d);
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
static void localsum(uint64_t itask, char *key, int keybytes, 
                     char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double *sum = *((double *) ptr);
  *sum += *((double *) value);
}

double MRVector::LocalSum()
{
  double sum = 0.;
  mr->map(mr, &localsum, &sum, 1);  // Set addflag to retain mr as is.
  return sum;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::LocalMin
// Compute min of all local vector entries.
static void localmin(uint64_t itask, char *key, int keybytes, 
                     char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double *min = ((double *) ptr);
  double v = *((double *) value);
  if (v < *min) *min = v;
}

double MRVector::LocalMin()
{
  double min = DBL_MAX;
  mr->map(mr, &localmin, &min, 1);  // Set addflag to retain mr as is.
  return min;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::LocalMax
// Compute max of all local vector entries.
static void localmax(uint64_t itask, char *key, int keybytes, 
                     char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double *max = ((double *) ptr);
  double v = *((double *) value);
  if (v > *max) *max = v;
}

double MRVector::LocalMax()
{
  double max = 0.;
  mr->map(mr, &localmax, &max, 1);  // Set addflag to retain mr as is.
  return max;
}

