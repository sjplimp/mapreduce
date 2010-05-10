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
#include <fstream>
#include <list>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "mrvector2.hpp"
#include "blockmacros.hpp"
#include "mrall2.h"
#include "localdisks.hpp"

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
             double alpha = 1., bool transpose = 0,
             int pagesize=MRMPI_MEMSIZE, const char *fpath=MYLOCALDISK);
    ~MRMatrix() {delete mr; delete emptyRows;};

    IDTYPE NumRows() { return N; };
    IDTYPE NumCols() { return M; };
    void MatVec(MRVector<IDTYPE> *, MRVector<IDTYPE> *, MRVector<IDTYPE> *);
    void Transpose();
    void Scale(double);
    void MakeEmpty() {delete mr;};
    MapReduce *mr;  // Actual storage; perhaps should be private.
    uint64_t nEmptyRows;  // Number of rows of A with no nonzeros.
    MapReduce *emptyRows; // Indices of rows of A with no nonzeros (leaf nodes).
    void Print(const char *filename = NULL);
    bool transposeFlag;  // State variable; indicates whether matrix is stored
                         // as A or as A^T.
    double scaleFactor;  // Factor by which all read/generated entries 
                         // are scaled.
  private:
    IDTYPE N;  // Number of rows
    IDTYPE M;  // Number of cols
    IDTYPE NNZ; // Number of nonzeros
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
// Store KVs by column index to make MatVec more efficient: 
// key = j, value = (i, a_ij).
// If transposeFlag is set, store A^T (i.e., store key=i, value=(j, a_ij)).
// Aggregate before returning from constructor.
// For rows with outdegree = 0, add a KV to the MapReduce object provided
// in ptr.
template <typename IDTYPE>
static void mrm_initialize_matrix(char *key, int keybytes, 
                  char *multivalue, int nvalues, int *valuebytes,
                  KeyValue *kv, void *ptr)
{
  // To receive entries for all-zero rows...
  MRMatrix<IDTYPE> *A = (MRMatrix<IDTYPE> *) ptr;
  KeyValue *emptyRowsKV = A->emptyRows->kv;
  bool transposeFlag = A->transposeFlag;

  uint64_t totalnvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, totalnvalues)
  uint64_t outdegree = totalnvalues - 1;

  if (outdegree != 0) {
    // Add matrix entries.
    MRNonZero<IDTYPE> v;
    v.nzv = A->scaleFactor / (double) outdegree;
    
    BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

    char *mptr = multivalue;
    for (int k = 0; k < nvalues; k++) {
      if (valuebytes[k] == 0) continue;  // NULL Entry for vertex
      if (transposeFlag) {
        // Storing by row index of A == column index of A^T.
        v.ij = *((IDTYPE *) mptr);
        kv->add(key, keybytes, (char *) &v, sizeof(MRNonZero<IDTYPE>));
      }
      else {
        // Storing by column index of A.
        v.ij = *((IDTYPE *) key);
        kv->add(mptr, valuebytes[k], (char *) &v, sizeof(MRNonZero<IDTYPE>));
      }
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
static void mrm_do_nothing_map(int itask, KeyValue *kv, void *ptr)
{
}

// Initial call to convert for a given MapReduce object incurs additional
// overhead; absorb that overhead here, returning the mr in the same state
// as before.  Assumes the mr was empty upon calling this function, so
// this function is actually never called.
static void mrm_do_nothing_reduce(char *key, int keybytes, char *multivalue,
                                  int nvalues, int *valuebytes,
                                  KeyValue *kv, void *ptr)
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
  double alpha,       // Factor by which to scale all entries.
  bool transpose,     // Flag indicating to store A^T instead of A.
  int pagesize,       // Optional:  MR pagesize to be set by the application.
  const char *fpath   // Optional:  MR filepath to be set by the application.
)
{
  transposeFlag = transpose;
  scaleFactor = alpha;
  // Create matrix MapReduce object mr.  
  mr = mredge->copy();
  mr->memsize = pagesize;
  mr->set_fpath(fpath);

  // Add in vertices so we can detect all-zero rows (leaf nodes in graph).
  mr->add(mrvert);

  // Create MapReduce object for all-zero rows.
  MapReduce *emr = new MapReduce(MPI_COMM_WORLD);
  emr->set_fpath(fpath);
  emr->memsize = pagesize;
  emr->map(1, mrm_do_nothing_map, NULL);
  
  // The initial call to convert incurs overhead for a mapreduce object.
  // We'll incur the overhead here so it doesn't pollute our computational
  // timings.
  emr->convert();  
  emr->reduce(mrm_do_nothing_reduce, NULL);

  // In mrm_initialize_matrix, we emit both to the matrix and to the
  // emptyRows MapReduce object so that we need only one pass over the edges.
  emptyRows = emr;
  emr->kv->append();
  mr->compress(mrm_initialize_matrix<IDTYPE>, this);
  emr->kv->complete();
  if (!transposeFlag) {
    // Need to aggregate by column index.  When transposeFlag, however,
    // mredge is already aggregated by row index, so the matrix will already
    // be aggregated correctly.
    mr->aggregate(NULL);
  }

  MPI_Allreduce(&(mr->kv->nkv), &NNZ, 1, MPI_UNSIGNED_LONG, MPI_SUM,
                MPI_COMM_WORLD);
 
  if (transposeFlag) {N = m; M = n;}
  else {N = n; M = m;}

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
    if (valuebytes[k] == sizeof(MRNonZero<IDTYPE>)) {
      MRNonZero<IDTYPE> *aptr = (MRNonZero<IDTYPE> *) mvptr;
      double product = x_j * aptr->nzv;
      kv->add((char *) &aptr->ij, sizeof(aptr->ij), 
              (char *) &product, sizeof(product));
      mvptr += valuebytes[k];
    }
    else {
      // This is the x_j value.
      // don't add in x_j * x_j
      mvptr += valuebytes[k];
      continue; 
    }
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
//  Deleting mr object and then copying from another mr
//  has a lot of overhead.  Just map x into y instead.
//  Calling map with this function is equivalent to
//     if (y->mr != NULL) delete y->mr;
//     y->mr = x->mr->copy();
//  but doesn't have the start-up overhead of a new mapreduce object.
static void copy_x(uint64_t itask, char *key, int keybytes,
                   char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  kv->add(key, keybytes, value, valuebytes);
}

//--------------------------------------------------------------
template <typename IDTYPE>
void MRMatrix<IDTYPE>::MatVec(
  MRVector<IDTYPE> *x,
  MRVector<IDTYPE> *y,       // Result of A*x
  MRVector<IDTYPE> *zerovec  // Vector with all row IDs and zero values.
)
{
  // Copy x->mr into y->mr and do processing in y->mr.  
  // Need to keep x->mr to compute residual.  
  // Plus, MatVec should not have side effect of changing x.

  y->mr->map(x->mr, copy_x, NULL);

  MapReduce *ymr = y->mr;

  // Add matrix terms to vector.
  // For A, merging Matrix row i and x_i.
  // For A^T, merging Matrix column j and x_j.
  ymr->add(mr);

  // For A, compute terms x_i * A_ij.
  // For A^T, compute terms x_j * A_ij.
  ymr->compress(mrm_terms<IDTYPE>, NULL);

  // Gather matrix now by rows.
  ymr->aggregate(NULL);
  
  // Need to add in zero terms to keep y-vector entries for all-zero columns.
  ymr->add(zerovec->mr);

  // Compute sum of terms over rows.
  ymr->compress(mrm_rowsum, NULL);
}


/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
// Print each matrix entry.
template <typename IDTYPE>
static void mrm_print(uint64_t itask, char *key, int keybytes, 
                      char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  int me;
  fstream *fout = (fstream *) ptr;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MRNonZero<IDTYPE> *v = (MRNonZero<IDTYPE> *) value;
  *fout << v->ij 
        << " " << *((IDTYPE *)key) 
        << " " << v->nzv << endl;
}

// Print each matrix-transpose entry.
template <typename IDTYPE>
static void mrm_print_transpose(uint64_t itask, char *key, int keybytes, 
                      char *value, int valuebytes, KeyValue *kv, void *ptr)
{
  int me;
  fstream *fout = (fstream *) ptr;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MRNonZero<IDTYPE> *v = (MRNonZero<IDTYPE> *) value;
  *fout << *((IDTYPE *)key) 
        << " " << v->ij 
        << " " << v->nzv << endl;
}

template <typename IDTYPE>
void MRMatrix<IDTYPE>::Print(const char *filename)
{
  fstream *fout;

  // Print header info
  if (filename) {
    // Matrix-Market header
    if (mr->my_proc() ==0) {
      char ff[267];
      sprintf(ff, "%s.header", filename);
      fstream fhead;
      fhead.open(ff, ios::out);
      fhead << "%%MatrixMarket matrix coordinate real general" << endl 
            << "%" << endl;
      fhead << N << " " << M << " " << NNZ << "\n";
      fhead.close();
    }
  }
  else {
    // Just print some stats.
    cout << "Matrix Info on processor " << mr->my_proc() 
         << " global nEmptyRows= " << nEmptyRows
         << " local NNZ= " << mr->kv->nkv << endl;
    cout << "Matrix Entries on processor " << mr->my_proc() << endl;
  }

  // Print matrix nonzeros.
  if (filename) {
    char ff[267];
    sprintf(ff, "%s.%03d", filename, mr->my_proc());
    fout = new fstream;
    fout->open(ff, ios::out);
  }
  else {
    fout = (fstream *) &cout;
  }
  if (!transposeFlag) 
    mr->map(mr, mrm_print<IDTYPE>, fout, 1);
  else
    mr->map(mr, mrm_print_transpose<IDTYPE>, fout, 1);

  if (filename) fout->close();
}

#endif
