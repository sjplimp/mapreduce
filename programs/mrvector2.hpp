// MapReduce Vector Class compatible with out-of-core MR-MPI.
//
// Stores a vector as a MapReduce object with 
// (Key, Value) = (Global Index i, vector value i)
//
// Karen Devine, 1416
// April 2010

#include <iostream>
#include <list>
#include <float.h>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "mrall2.h"

using namespace MAPREDUCE_NS;
using namespace std;

#ifndef __MRVECTOR
#define __MRVECTOR

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

template <typename IDTYPE>
class MRVector {
  public:
    MRVector(IDTYPE, int pagesize=64, const char *fpath = ".");
    ~MRVector() {delete mr;}
    void MakeEmpty();
    void PutScalar(double);
    void AddScalar(double);
    void Scale(double);
    double GlobalSum();
    double GlobalMax();
    double GlobalMin();
    double LocalSum();
    double LocalMax();
    double LocalMin();
    IDTYPE GlobalLen() {return global_len;};
    void Print();
    MapReduce *mr;   // Where the data is actually stored; probably should
                     // be private.
  private:
    IDTYPE global_len;
};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////


/////////////////////////////////////////////////////////////////////////////
// initialize_vec map() function
// Initializes a vector; emits its values to be hashed to processors by index.
// Vector entries are initialized to zero.
// Output:  Key = global index;  Value = 0.
template <typename IDTYPE>
static void mrv_initialize_vec(int itask, KeyValue *kv, void *ptr)
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
// Empty a vector of all entries, but don't delete the mr object.
static void mrv_do_nothing_map(int itask, KeyValue *kv, void *ptr)
{

}

static void mrv_do_nothing_reduce(char *key, int keybytes, char *multivalue,
                                  int nvalues, int *valuebytes,
                                  KeyValue *kv, void *ptr)
{

}

template <typename IDTYPE>
void MRVector<IDTYPE>::MakeEmpty()
{
  mr->map(1, mrv_do_nothing_map, NULL);
}

/////////////////////////////////////////////////////////////////////////////
// Vector constructor.  Allocates memory and stores in persistent memory.
// Initializes vector uniformly to 1/n.
template <typename IDTYPE>
MRVector<IDTYPE>::MRVector(
  IDTYPE n,                // Total number of vector entries 
  int pagesize,            // Optional:  MR memsize
  const char *fpath        // Optional:  MR fpath
)
{
  if (n == 0) {
    cout << "Invalid vector length 0" << endl;
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  global_len = n;

  mr = new MapReduce(MPI_COMM_WORLD);
  mr->memsize = pagesize;
  mr->set_fpath(fpath);

  // First calls to map and convert incur overhead; we'll incur that
  // cost here rather than have it pollute our computation timings.
  mr->map(1, mrv_do_nothing_map, NULL);
  mr->convert();
  mr->reduce(mrv_do_nothing_reduce, NULL);

  // Emit vector values
  mr->map(mr->num_procs(), mrv_initialize_vec<IDTYPE>, &n);

  // Gather values to processors based on vector index.
  mr->aggregate(NULL);
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::Print
// Print the vector entries; nothing fancy for parallel here.
template <typename IDTYPE>
static void mrv_printvector(uint64_t itask, char *key, int keybytes,
                        char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  int me;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  cout << me << " " << *((IDTYPE *) key) << "\t" << *((double *) value) << endl;
}

template <typename IDTYPE>
void MRVector<IDTYPE>::Print()
{
  int me = mr->my_proc();
  cout << "Vector on processor " << me 
       << " Local Length = " << mr->kv->nkv << endl;
  mr->map(mr, mrv_printvector<IDTYPE>, NULL, 1);  // set addflag to keep mr as is.
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::PutScalar
// Assign a scalar value to each entry of a vector.
static void mrv_setvector(uint64_t itask, char *key, int keybytes, 
                      char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double d = *((double *) ptr);
  kv->add(key, keybytes, (char *) &d, sizeof(double));
}

template <typename IDTYPE>
void MRVector<IDTYPE>::PutScalar(double d)
{
  mr->map(mr, mrv_setvector, &d);
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::Scale
// Multiply each vector entry by a scalar value.
static void mrv_scalevector(uint64_t itask, char *key, int keybytes, 
                        char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double d = *((double *) ptr);
  double v = *((double *) value) * d;
  kv->add(key, keybytes, (char *) &v, sizeof(double));
}

template <typename IDTYPE>
void MRVector<IDTYPE>::Scale(double d)
{
  mr->map(mr, mrv_scalevector, &d);
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::AddScalar
// Add a scalar to each vector entry.
static void mrv_addscalarvector(uint64_t itask, char *key, int keybytes, 
                           char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double d = *((double *) ptr);
  double v = *((double *) value) + d;
  kv->add(key, keybytes, (char *) &v, sizeof(double));
}

template <typename IDTYPE>
void MRVector<IDTYPE>::AddScalar(double d)
{
  mr->map(mr, mrv_addscalarvector, &d);
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::GlobalMin
// Compute min of all vector entries.
// Requires communication
// Cheating by using MPI_Allreduce instead of MapReduce.
template <typename IDTYPE>
double MRVector<IDTYPE>::GlobalMin()
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
template <typename IDTYPE>
double MRVector<IDTYPE>::GlobalMax()
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
template <typename IDTYPE>
double MRVector<IDTYPE>::GlobalSum()
{
  double sum = LocalSum(); 
  double gsum;
  MPI_Allreduce(&sum, &gsum, 1, MPI_DOUBLE, MPI_SUM, mr->communicator());
  return gsum;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::LocalSum
// Compute sum of all local vector entries.
static void mrv_localsum(uint64_t itask, char *key, int keybytes, 
                     char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double *sum = (double *) ptr;
  *sum += *((double *) value);
}

template <typename IDTYPE>
double MRVector<IDTYPE>::LocalSum()
{
  double sum = 0.;
  mr->map(mr, mrv_localsum, &sum, 1);  // Set addflag to retain mr as is.
  return sum;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::LocalMin
// Compute min of all local vector entries.
static void mrv_localmin(uint64_t itask, char *key, int keybytes, 
                     char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double *min = ((double *) ptr);
  double v = *((double *) value);
  if (v < *min) *min = v;
}

template <typename IDTYPE>
double MRVector<IDTYPE>::LocalMin()
{
  double min = DBL_MAX;
  mr->map(mr, mrv_localmin, &min, 1);  // Set addflag to retain mr as is.
  return min;
}

/////////////////////////////////////////////////////////////////////////////
// MRVector::LocalMax
// Compute max of all local vector entries.
static void mrv_localmax(uint64_t itask, char *key, int keybytes, 
                     char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double *max = ((double *) ptr);
  double v = *((double *) value);
  if (v > *max) *max = v;
}

template <typename IDTYPE>
double MRVector<IDTYPE>::LocalMax()
{
  double max = 0.;
  mr->map(mr, mrv_localmax, &max, 1);  // Set addflag to retain mr as is.
  return max;
}

#endif
