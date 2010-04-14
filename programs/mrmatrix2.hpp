// MapReduce Matrix class
// Karen Devine, 1416
// April 2010
//
// Store matrix as a MapReduce object, with different storage depending on
// whether matrix A is stored or transpose A^T is stored.
// For A:  (Key, Value) = (row index i, (col index j, a_ij))
// For A^T:  (Key, Value) = (col index j, (row index i, a_ij))
// 
// Provides method for matrix-vector multiplication  A*x=y.
//
// The dimensions of the matrix A are given by N and M (N rows, M columns).

#include <iostream>
#include <list>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "mrvector2.hpp"
#include "blockmacros.hpp"
#include "mrall2.h"

using namespace MAPREDUCE_NS;
using namespace std;

#ifndef __MRMATRIX
#define __MRMATRIX

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

template <typename IDTYPE>
class MRNonZero {
public:
  IDTYPE ij;        // column (if A) or row (if A^T) index
  double nzv;       // non-zero value.
  MRNonZero() {};
  ~MRNonZero() {};
};

template <typename IDTYPE>
class MRMatrix {
  public:  
    MRMatrix(IDTYPE, IDTYPE, MapReduce *, MapReduce *, 
             int pagesize=64, const char *fpath=".");
    ~MRMatrix() {delete mr;};

    IDTYPE NumRows() { return N; };
    IDTYPE NumCols() { return M; };
    void MatVec(MRVector<IDTYPE> *, MRVector<IDTYPE> *);
    void Transpose();
    void Scale(double);
    void MakeEmpty() {delete mr;};
    MapReduce *mr;  // Actual storage; perhaps should be private.
    uint64_t nEmptyRows;  // Number of rows of A with no nonzeros.
    MapReduce *emptyRows; // Indices of rows of A with no nonzeros (leaf nodes).
  private:
    IDTYPE N;  // Number of rows
    IDTYPE M;  // Number of cols
    bool transposeFlag;  // State variable; indicates whether matrix is stored
                         // as A or as A^T.
};


/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////
// Matrix constructor. 
// Takes a graph as input and constructs an appropriate matrix.
//
// initialize_matrix map function
// Given MapReduce object containing edges and vertices, 
// create matrix non-zeros.  Give initial value of 1/(outdegree of v_i) to
// each nonzero in row i.  
// For rows with outdegree = 0, add a KV to the MapReduce object provided
// in ptr.
template <typename IDTYPE>
static void mrm_initialize_matrix(char *key, int keybytes, 
                  char *multivalue, int nvalues, int *valuebytes,
                  KeyValue *kv, void *ptr)
{
  // To receive entries for all-zero rows...
  KeyValue *emptyRowsKV = (KeyValue *) ptr;

  uint64_t totalnvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, totalnvalues)
  uint64_t outdegree = totalnvalues - 1;

  if (outdegree != 0) {
    // Add matrix entries.
    MRNonZero<IDTYPE> v;
    v.nzv = 1. / (double) outdegree;
    
    BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

    char *mptr = multivalue;
    for (int k = 0; k < nvalues; k++) {
      if (valuebytes[k] == 0) continue;  // NULL Entry for vertex
      v.ij = *((IDTYPE *) mptr);
      kv->add(key, keybytes, (char *) &v, sizeof(MRNonZero<IDTYPE>));
      mptr += valuebytes[k];
    }
    
    END_BLOCK_LOOP
  }
  else {
    // All Zero row; add entry to emptyRowsKV for this row.
    const double zero = 0.;
    emptyRowsKV->add(key, keybytes, (char *) &zero, sizeof(double));
  }
}

//--------------------------------------------------------------
// We need emptyRows MapReduce object to be in state where it has a KeyValue
// structure, but we don't have anything to put in it yet.  This function 
// will do the trick.
static void mrm_do_nothing(int itask, KeyValue *kv, void *ptr)
{
}

                       
//--------------------------------------------------------------
template <typename IDTYPE>
MRMatrix<IDTYPE>::MRMatrix(
  IDTYPE n,           // Number of matrix rows 
  IDTYPE m,           // Number of matrix columns 
  MapReduce *mrvert,  // Vertices of the graph == matrix rows.
                      // Assuming mrvert is already aggregated to processors.
  MapReduce *mredge,  // Edges of the graph == matrix nonzeros.
                      // Assuming mredge is already aggregated to processors.
  int pagesize,       // Optional:  MR pagesize to be set by the application.
  const char *fpath   // Optional:  MR filepath to be set by the application.
)
{
  // Create matrix MapReduce object mr.  
  mr = mredge->copy();
  mr->memsize = pagesize;
  mr->set_fpath(fpath);

  // Add in vertices so we can detect all-zero rows (leaf nodes in graph).
  mr->add(mrvert);

  // Create MapReduce object for all-zero rows.
  MapReduce *emr = new MapReduce(MPI_COMM_WORLD);
  emr->map(1, mrm_do_nothing, NULL);

  // In mrm_initialize_matrix, we emit both to the matrix and to the
  // emptyRows MapReduce object so that we need only one pass over the edges.
  emr->kv->append();
  mr->compress(mrm_initialize_matrix<IDTYPE>, emr->kv);
  emr->kv->complete();
 
  N = n; M = m;
  MPI_Allreduce(&emr->kv->nkv, &nEmptyRows, 1, MPI_UNSIGNED_LONG, MPI_SUM,
                MPI_COMM_WORLD);
}


/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
// Transpose the matrix.  Re-aggregate it based on new indices.
//
template <typename IDTYPE>
static void mrm_transpose_matrix(uint64_t itask, char *key, int keybytes,
                                 char *value, int valuebytes,
                                 KeyValue *kv, void *ptr)
{
  MRNonZero<IDTYPE> *v = (MRNonZero<IDTYPE> *) value;
  IDTYPE tmp = v->ij;
  v->ij = *((IDTYPE *) key);
  kv->add((char *) &tmp, sizeof(IDTYPE), (char *) v, sizeof(MRNonZero<IDTYPE>));
}

template <typename IDTYPE>
void MRMatrix<IDTYPE>::Transpose() 
{
  transposeFlag = !transposeFlag;
  mr->map(mr, mrm_transpose_matrix<IDTYPE>, NULL);
  mr->aggregate(NULL);
}


/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
// Scale each matrix entry by value alpha.
template <typename IDTYPE>
static void mrm_scalematrix(uint64_t itask, char *key, int keybytes, 
                        char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  double d = *((double *) ptr);
  MRNonZero<IDTYPE> *v = (MRNonZero<IDTYPE> *) value;
  v->nzv *= d;
  kv->add(key, keybytes, value, valuebytes);
}

template <typename IDTYPE>
void MRMatrix<IDTYPE>::Scale(
  double alpha
)
{
  mr->map(mr, mrm_scalematrix<IDTYPE>, &alpha);
}


/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
// matrix-vector multiplication function:  A * x = y.
// The result is stored in y's MapReduce object.
//
// terms reduce() function
// input:  key = column index; multivalue = {x_j, of A_ij for
//         all i with nonzero A_ij.}
// output:  key = row index i; value = x_j * A_ij.

template <typename IDTYPE>
static void mrm_terms(char *key, int keybytes, 
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
  int k;
  for (k = 0; k < nvalues; k++) {
    if (valuebytes[k] != sizeof(MRNonZero<IDTYPE>))
      break;
    mvptr += valuebytes[k];
  }
  if (k < nvalues) BREAK_BLOCK_LOOP;

  END_BLOCK_LOOP
  
  double x_j = *((double *) mvptr);

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  mvptr = multivalue;
  for (int k = 0; k < nvalues; k++) {
    if (valuebytes[k] != sizeof(MRNonZero<IDTYPE>)) {
      mvptr += valuebytes[k];
      continue; // don't add in x_j * x_j
    }
    MRNonZero<IDTYPE> *aptr = (MRNonZero<IDTYPE> *) mvptr;
    double product = x_j * aptr->nzv;
    kv->add((char *) &aptr->ij, sizeof(aptr->ij), 
            (char *) &product, sizeof(product));
    mvptr += valuebytes[k];
  }

  END_BLOCK_LOOP
}

//--------------------------------------------------------------
// rowsum reduce() function
// input:  key = row index i; 
//         multivalue = {x_j*A_ij for all j with nonzero A_ij}.
//         y vector in ptr argument (optional; ptr == NULL is allowed.)
// output: if ptr != NULL, vector entries in vector y
//         else key,value pairs == row, value.
static void mrm_rowsum(char *key, int keybytes, char *multivalue, 
                   int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  double sum = 0.;

  uint64_t totalnvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, totalnvalues)
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  double *dptr = (double *) multivalue;
  for (int k = 0; k < nvalues; k++) 
    sum += dptr[k];

  END_BLOCK_LOOP

  kv->add(key, keybytes, (char *) &sum, sizeof(sum));
}

//--------------------------------------------------------------
template <typename IDTYPE>
void MRMatrix<IDTYPE>::MatVec(
  MRVector<IDTYPE> *x,
  MRVector<IDTYPE> *y      // Result of A*x
)
{
  // Copy x->mr into y->mr and do processing in y->mr.  
  // Need to keep x->mr to compute residual.  
  // Plus, MatVec should not have side effect of changing x.
  if (y->mr != NULL) delete y->mr;
  y->mr = x->mr->copy();

cout << "    KDDKDD IN MATVEC After copy " << endl;
y->Print();

  MapReduce *ymr = y->mr;
  
  // Add matrix terms to vector.
  // For A, merging Matrix row i and x_i.
  // For A^T, merging Matrix column j and x_j.
  ymr->add(mr);

cout << "    KDDKDD IN MATVEC After add " << endl;
y->Print();

  // For A, compute terms x_i * A_ij.
  // For A^T, compute terms x_j * A_ij.
  ymr->compress(mrm_terms<IDTYPE>, NULL);

cout << "    KDDKDD IN MATVEC After compress " << endl;
y->Print();

  // Gather matrix now by rows.
  ymr->collate(NULL);

  // Compute sum of terms over rows.
  ymr->reduce(mrm_rowsum, NULL);

cout << "    KDDKDD IN MATVEC After reduce " << endl;
y->Print();

}

#endif
