// MapReduce Matrix Class
// Karen Devine, 1416
// June 2008
//
// The dimensions of the matrix A are given by N and M (N rows, M columns).

#include <iostream>
#include <list>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "mrmatrix2.h"
#include "mrvector2.h"

using namespace MAPREDUCE_NS;
using namespace std;

/////////////////////////////////////////////////////////////////////////////
// Matrix constructor. 
// Takes a graph as input and constructs an appropriate matrix.
//
// initialize_matrix map function
// Given MapReduce object containing edges, create matrix non-zeros.
template <typename IDTYPE>
static void initialize_matrix(uint64_t itask, char *key, int keybytes,
                              char *value, int valuebytes, 
                              KeyValue *kv, void *ptr)
{
  MRNonZero v;
  v.ij = *((IDTYPE *) value);
  v.nzv = 0.;
  kv->add(key, keybytes, (char *) &v, sizeof(MRNonZero));
}

//--------------------------------------------------------------
template <typename IDTYPE>
static void initialize_transpose_matrix(uint64_t itask, char *key, int keybytes,
                                        char *value, int valuebytes,
                                        KeyValue *kv, void *ptr)
{
  MRNonZero v;
  v.ij = *((IDTYPE *) key);
  v.nzv = 0.;
  kv->add(value, valuebytes, (char *) &v, sizeof(MRNonZero));
}

//--------------------------------------------------------------
static void save_empty_rows(char *key, int keybytes, 
                            char *multivalue, int nvalues, int *valuebytes,
                            KeyValue *kv, void *ptr)
{
  double zero = 0.;
  if (nvalues == 1) { 
    // No edges originating at this vertex; matrix row is all zeros.
    // This is an empty row; keep it.
    kv->add(key, keybytes, (char *) &zero, sizeof(double));
  }
}
                       
//--------------------------------------------------------------
template <typename IDTYPE>
MRMatrix::MRMatrix(
  IDTYPE n,           // Number of matrix rows 
  IDTYPE m,           // Number of matrix columns 
  MapReduce *mredge,  // Edges of the graph == matrix nonzeros.
                      // Assuming mredge is already aggregated to processors.
  bool transpose,     // Store matrix A or matrix transpose A^T?
  int pagesize,       // Optional:  MR pagesize to be set by the application.
  char *filepath      // Optional:  MR filepath to be set by the application.
)
{
  // Create matrix MapReduce object mr.  Store as A or A^T, depending on
  // transpose flag.
  mr = new MapReduce(MPI_COMM_WORLD);
  mr->memsize = pagesize;
  mr->set_fpath(filepath);

  transposeFlag = transpose;
  if (transpose) {
    N = m; M = n;
    mr->map(mredge, &initialize_transpose_matrix<IDTYPE>, NULL);
  }
  else {
    N = n; M = m;
    mr->map(mredge, &initialize_matrix<IDTYPE>, NULL);
  }

  // Identify empty rows of matrix A (leaf nodes in graph).
  emptyRows = new MRVector(N, pagesize, filepath);
  MapReduce *emr = emptyRows->mr;
  emr->add(mredge);
  nEmptyRows = emr->compress(&save_empty_rows, NULL);
}

/////////////////////////////////////////////////////////////////////////////
// Scale each matrix entry by value alpha.
static void scalematrix(uint64_t itask, char *key, int keybytes, 
                        char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double d = *((double *) ptr);
  MRNonZero *v = (MRNonZero *) value;
  v->nzv *= d;
  kv->add(key, keybytes, value, valuebytes);
}

void MRMatrix::Scale(
  double alpha
)
{
  mr->map(mr, scalematrix, &alpha);
}


/////////////////////////////////////////////////////////////////////////////
// matrix-vector multiplication function:  A * x = y.
// The result is stored in y's MapReduce object.
//
// terms reduce() function
// input:  key = column index; multivalue = {x_j, of A_ij for
//         all i with nonzero A_ij.}
// output:  key = row index i; value = x_j * A_ij.

static void terms(char *key, int keybytes, 
                  char *multivalue, int nvalues, int *valuebytes,
                  KeyValue *kv, void *ptr)
{

  if (nvalues == 1) {
    // No nonzeros in this column; just re-emit x_j.
    double zero = 0;
    kv->add(key, keybytes, (char *) &zero, sizeof(double));
    return;
  }

  uint64_t totalnvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, totalnvalues)

  char *mvptr;
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  mvptr = multivalue;
  // Find the x_j value
  for (int k = 0; k < nvalues; k++) {
    if (valuebytes[k] != sizeof(MRNonZero))
      break;
    mvptr += valuebytes[k];
  }
  if (k < nvalues) BREAK_BLOCK_LOOP;

  END_BLOCK_LOOP
  
  double x_j = *((double *) mvptr);

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  mvptr = multivalue;
  for (int k = 0; k < nvalues; k++) {
    if (valuebytes[k] != sizeof(MRNonZero)) {
      mvptr += valuebytes[k];
      continue; // don't add in x_j * x_j
    }
    MRNonZero *aptr = (MRNonZero *) mvptr;
    double product = x_j * aptr->nzv;
    kv->add((char *) &aptr->ij, sizeof(aptr->ij), 
            (char *) &product, sizeof(product));
    mvptr += valuebytes[k];
  }

  END_BLOCK_LOOP
}

// rowsum reduce() function
// input:  key = row index i; 
//         multivalue = {x_j*A_ij for all j with nonzero A_ij}.
//         y vector in ptr argument (optional; ptr == NULL is allowed.)
// output: if ptr != NULL, vector entries in vector y
//         else key,value pairs == row, value.
static void rowsum(char *key, int keybytes, char *multivalue, 
                   int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  double sum = 0.;

  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, totalnvalues)
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  double *dptr = (double *) multivalue;
  for (int k = 0; k < nvalues; k++) 
    sum += dptr[k];

  END_BLOCK_LOOP

  kv->add(key, keybytes, (char *) &sum, sizeof(sum));
}

template <typename IDTYPE>
void MRMatrix::MatVec(
  MRVector<IDTYPE> *x,
  MRVector<IDTYPE> *y      // Result of A*x
)
{
  // Don't know whether or not I need these; perhaps the copy takes care of
  // deleting old KVs and setting up new ones.
  // delete y->mr;
  // y->mr = new MapReduce(MPI_COMM_WORLD);

  MapReduce *ymr = y->mr;
  ymr->copy(x->mr);
  
  // Add matrix terms to vector.
  // For A, merging Matrix row i and x_i.
  // For A^T, merging Matrix column j and x_j.
  ymr->add(mr);

  // For A, compute terms x_i * A_ij.
  // For A^T, compute terms x_j * A_ij.
  ymr->compress(&terms, NULL);

  // Gather matrix now by rows.
  ymr->collate(NULL);

  // Compute sum of terms over rows.
  ymr->reduce(&rowsum, NULL);
}
