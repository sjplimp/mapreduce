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
#include "mrmatrix.h"
#include "mrvector.h"

using namespace MAPREDUCE_NS;
using namespace std;

//  MAP FUNCTIONS
MAPFILEFUNCTION store_matrix_directly;
MAPFILEFUNCTION initialize_matrix;
MAPFUNCTION emit_matrix_entries;
MAPFUNCTION emit_matvec_matrix;
MAPFUNCTION emit_matvec_vector;
MAPFUNCTION emit_matvec_empty_terms;
MAPFUNCTION local_terms;

//  REDUCE FUNCTIONS
REDUCEFUNCTION store_matrix_by_map;
REDUCEFUNCTION terms;
REDUCEFUNCTION rowsum;

/////////////////////////////////////////////////////////////////////////////
// Comparison functions for matrix entries.
bool sort_by_row(const MatrixEntry &lhs, const MatrixEntry &rhs)
{
  return lhs.i < rhs.i;
}

bool sort_by_col(const MatrixEntry &lhs, const MatrixEntry &rhs)
{
  return lhs.j < rhs.j;
}

/////////////////////////////////////////////////////////////////////////////
// Matrix constructor.  Reads matrix from files; stores in persistent memory.
// Does not emit anything, but uses Map to read the files.
MRMatrix::MRMatrix(
  MapReduce *mr,
  int n,          // Number of matrix rows 
  int m,          // Number of matrix columns 
  char *filename, // Base filename; NULL if want to re-use existing Amat.
  int storage,    // Flag indicating whether to remap data by rows or cols
                  // before storing on processors.  
  bool storage_aware // Use storage-aware algorithm if possible.
)
{
  N = n;
  M = m;
  storageFormat = storage;

  if (storage != BY_FILE) {
    // Read matrix-market files; emit values with row number as index.
    int nnz = mr->map(mr->num_procs(), 1, &filename, '\n', 40,
                      &initialize_matrix, (void *)this, 0);
    // Gather rows to processors.
    mr->collate(NULL);
    // Store matrix by rows on processors.
    mr->reduce(&store_matrix_by_map, (void *)this);
    if (storage_aware)
      if (storageFormat == BY_COL) 
        Amat.sort(sort_by_col); // sort by column for easy matvec
      else
        Amat.sort(sort_by_row); // sort by row for easy matvec
  }
  else {
    // storage = BY_FILE
    // Read matrix-market files; store it in the proc that reads it.
    int nnz = mr->map(mr->num_procs(), 1, &filename, '\n', 40, 
                      &store_matrix_directly, (void *)this, 0);
  }
}

/////////////////////////////////////////////////////////////////////////////
// Scale each matrix entry by value alpha.
void MRMatrix::Scale(
  double alpha
)
{
  list<MatrixEntry>::iterator nz;
  for (nz=Amat.begin(); nz!=Amat.end(); nz++) 
     (*nz).nzv *= alpha;
}


/////////////////////////////////////////////////////////////////////////////
// matrix-vector multiplication function:  A * x = y.
// If y is non-null, the result is stored in y.
// If y is null, the result is emitted to the MapReduce object.

void MRMatrix::MatVec(
  MapReduce *mr,
  MRVector *x,
  MRVector *y,     // Result of A*x; optional.
  bool transpose,  // Flag indicating whether to do A*x or A^T * x.
  bool storage_aware // Use storage-aware algorithm if possible.
)
{
  int me = mr->my_proc();
  int np = mr->num_procs();

  transposeFlag = transpose;

  if (storage_aware && x->StorageFormat() &&
      ((storageFormat == BY_ROW && transposeFlag) ||  
      (storageFormat == BY_COL && !transposeFlag))) {
    // needed vector and matrix entries are on same proc;
    // don't need to move them around before matvec.
    struct {
      MRMatrix *A;
      MRVector *x;
    } tmp;
    tmp.A = this;
    tmp.x = x;
    int nterms = mr->map(np, &local_terms, &tmp, 0);
  }
  else {
    // Data is not stored the way we want to use it for matvec.
    // Need to reorganize.
    // Emit matrix values.
    int nnz = mr->map(np, &emit_matvec_matrix, (void *)this, 0);

    // Emit vector values.
    mr->map(np, &emit_matvec_vector, (void *)x, 1);

    // Gather matrix column j and x_j to same processors.
    mr->collate(NULL);

    // Compute terms x_j * A_ij.
    int nterms = mr->reduce(&terms, NULL);
  }

  // Even if A is sparse, want resulting product vector to be dense.
  // Emit some dummies to make the product vector dense.
  // These are dummy terms in the rowsum that will cause product
  // vector entries to be added.
  if (transpose) 
    mr->map(NumCols(), &emit_matvec_empty_terms, NULL, 1);
  else
    mr->map(NumRows(), &emit_matvec_empty_terms, NULL, 1);

  // Gather matrix now by rows.
  mr->collate(NULL);

  // Compute sum of terms over rows.
  if (y) y->MakeEmpty();
  int nrow = mr->reduce(&rowsum, y);
}

/////////////////////////////////////////////////////////////////////////////
// emit_matvec_vector map() function
// For each entry in input MRVector, emit (key,value) = (i,[-1,v_i]).
void emit_matvec_vector(int itask, KeyValue *kv, void *ptr)
{
  // Assume ptr = MRVector.
  MRVector *x = (MRVector *) ptr;
  INTDOUBLE value;

  list<INTDOUBLE>::iterator v;
  for (v = x->vec.begin(); v != x->vec.end(); v++) {
    value.i = XVECVALUE;
    value.d = (*v).d;
//printf("DkkDkk  emit_matvec_vector %d %f\n", (*v).i, value.d);
    kv->add((char *)&((*v).i), sizeof((*v).i), (char *) &value, sizeof(value));
  }
}

/////////////////////////////////////////////////////////////////////////////
// emit_matvec_matrix map() function
// For each non-zero in MRMatrix, emit (key,value) = (j,[A_ij,i]).
// For transpose, emit (key,value) = (i,[A_ij,j]).
void emit_matvec_matrix(int itask, KeyValue *kv, void *ptr)
{
  MRMatrix *A = (MRMatrix *) ptr;
  bool transpose = A->UseTranspose();

  list<MatrixEntry>::iterator nz;
  for (nz=A->Amat.begin(); nz!=A->Amat.end(); nz++) {
    INTDOUBLE value;
    value.d = (*nz).nzv;
    if (transpose) {
      value.i = (*nz).j;
//printf("DkkDkk  emit_matvec_matrix %d %d %f\n", (*nz).i, (*nz).j, (*nz).nzv);
      kv->add((char *)&((*nz).i), sizeof((*nz).i), 
              (char *)&value, sizeof(value));
    }
    else {
      value.i = (*nz).i;
      kv->add((char *)&((*nz).j), sizeof((*nz).j), 
              (char *)&value, sizeof(value));
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
// emit_matvec_empty_terms map() function
// For each itask, emit (key,value) = (i,0)
void emit_matvec_empty_terms(int itask, KeyValue *kv, void *ptr)
{
  double zero = 0.;
  int row = itask+1;  // Matrix-market is one-based.
// printf("  kDDkDD emit_matvec_empty_terms %d %f\n", row, zero);
  kv->add((char *)&row, sizeof(row), (char *) &zero, sizeof(zero));
}


/////////////////////////////////////////////////////////////////////////////
// initialize_matrix map() function
// Read matrix from matrix-market file.
// Assume matrix-market file is split into itask chunks.
// To allow re-use of the matrix without re-reading the files, 
// store the nonzeros read by this processor.

void initialize_matrix(int itask, char *bytes, int nbytes, KeyValue *kv, 
                       void *ptr)
{
  MRMatrix *A = (MRMatrix *) ptr;

  A->MakeEmpty();

  int i, j;
  double nzv;
  char line[81];
  // Read matrix market file.  Emit (key, value) = (j, [A_ij,i]).
  int linecnt = 0;
  for (int k = 0; k < nbytes-1; k++) {
    line[linecnt++] = bytes[k];
    if (bytes[k] == '\n') {
      if (line[0] != '%') {  // i.e., not a comment line.
        sscanf(line, "%d %d %lf", &i, &j, &nzv);
        if (nzv <= 1.) {
          // Valid matrix entry for pagerank problem have nzv <= 1.
          // Not general for all problems!!!!
          MatrixEntry nz;
          nz.i = i;
          nz.j = j;
          nz.nzv = nzv;
          if (A->StorageFormat() == BY_ROW)
            kv->add((char *)&i, sizeof(i), (char *)&nz, sizeof(nz));
          else
            kv->add((char *)&j, sizeof(j), (char *)&nz, sizeof(nz));
        }
        else {
          // Valid matrix entry for pagerank problem have nzv <= 1.
          // Not general for all problems!!!!
          cout << "Skipping line with values (" << i << ", " 
               << j << ") == " << nzv << endl;
        }
      }
      linecnt = 0;
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
// store_matrix_by_map reduce() function
// Having received row-indexed matrix entries, the processor stores the
// entries.

void store_matrix_by_map(char *key, int keylen, char *multivalue, int nvalues, 
                         int *mvlen, KeyValue *kv, void *ptr)
{
  MRMatrix *A = (MRMatrix *) ptr;
  int row = *((int *)key);
  MatrixEntry *nz = (MatrixEntry *)multivalue;

  for (int k = 0; k < nvalues; k++, nz++) 
    A->AddNonzero(nz->i, nz->j, nz->nzv);
}

/////////////////////////////////////////////////////////////////////////////
// store_matrix_directly map() function
// Read matrix from matrix-market file.
// Assume matrix-market file is split into itask chunks.
// To allow re-use of the matrix without re-reading the files, 
// store the nonzeros read by this processor.

void store_matrix_directly(int itask, char *bytes, int nbytes, KeyValue *kv, 
                           void *ptr)
{
  MRMatrix *A = (MRMatrix *) ptr;

  A->MakeEmpty();

  int i, j;
  double nzv;
  char line[81];
  // Read matrix market file.  Emit (key, value) = (j, [A_ij,i]).
  int linecnt = 0;
  for (int k = 0; k < nbytes-1; k++) {
    line[linecnt++] = bytes[k];
    if (bytes[k] == '\n') {
      if (line[0] != '%') {  // i.e., not a comment line.
        sscanf(line, "%d %d %lf", &i, &j, &nzv);
        if (nzv <= 1.) {
          // Valid matrix entry for pagerank problem have nzv <= 1.
          // Not general for all problems!!!!
          A->AddNonzero(i, j, nzv);
        }
        else {
          // Valid matrix entry for pagerank problem have nzv <= 1.
          // Not general for all problems!!!!
          cout << "Skipping line with values (" << i << ", " 
               << j << ") == " << nzv << endl;
        }
      }
      linecnt = 0;
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
// local_terms map() function
// input:  ptr with A, x.
// output: key = row index i; value = x_j * A_ij for each local A_ij.

void local_terms(int itask, KeyValue *kv, void *ptr)
{
  struct matpoint{
    MRMatrix *A;
    MRVector *x;
  };
  struct matpoint *tmp = (struct matpoint *) ptr;
  MRMatrix *A = tmp->A;
  MRVector *x = tmp->x;
  list<MatrixEntry>::iterator nz;
  list<INTDOUBLE>::iterator v;
  v = x->vec.begin();
  if (A->UseTranspose()) {
    // Would only be in local_terms if stored by rows and sorted by rows.
    for (nz=A->Amat.begin(); nz!=A->Amat.end(); nz++) {
      while ((*v).i != (*nz).i && v != x->vec.end()) v++;
      double product = (*v).d * (*nz).nzv;
 //  printf("  kDDkDD local_terms %d %f\n", (*nz).j, product);
      kv->add((char *) &((*nz).j), sizeof((*nz).j), 
              (char *) &product, sizeof(product));
    }
  }
  else {
    // Would only be in local_terms if stored by cols and sorted by cols.
    for (nz=A->Amat.begin(); nz!=A->Amat.end(); nz++) {
      while ((*v).i != (*nz).j && v != x->vec.end()) v++;
      double product = (*v).d * (*nz).nzv;
//   printf("  kDDkDD local_terms %d %f\n", (*nz).i, product);
      kv->add((char *) &((*nz).i), sizeof((*nz).i), 
              (char *) &product, sizeof(product));
    }
  }
}
/////////////////////////////////////////////////////////////////////////////
// terms reduce() function
// input:  key = column index; multivalue = {x_j, of A_ij for
//         all i with nonzero A_ij.}
// output:  key = row index i; value = x_j * A_ij.

void terms(char *key, int keylen, char *multivalue, int nvalues, int *mvlen,
           KeyValue *kv, void *ptr)
{

  if (nvalues == 1) 
    // No nonzeros in this column; skip it.
    return;

  // Find the x_j value
  INTDOUBLE *xptr = (INTDOUBLE *) multivalue;
  for (int k = 0; k < nvalues; k++, xptr++) {
    if (xptr->i < 0)  // found it
      break;
  }
  double x_j = xptr->d;

  INTDOUBLE *aptr = (INTDOUBLE *) multivalue;
  for (int k = 0; k < nvalues; k++, aptr++) {
    if (aptr == xptr) continue;  // don't add in x_j * x_j.

    double product = x_j * aptr->d;
//printf("  kDDkDD terms %d %f\n", aptr->i, product);
    kv->add((char *) &aptr->i, sizeof(aptr->i), 
            (char *) &product, sizeof(product));
  }
}

/////////////////////////////////////////////////////////////////////////////
// rowsum reduce() function
// input:  key = row index i; 
//         multivalue = {x_j*A_ij for all j with nonzero A_ij}.
//         y vector in ptr argument (optional; ptr == NULL is allowed.)
// output: if ptr != NULL, vector entries in vector y
//         else key,value pairs == row, value.
void rowsum(char *key, int keylen, char *multivalue, int nvalues, int *mvlen,
            KeyValue *kv, void *ptr)
{
  double sum = 0;
  int row = *(int*) key;
  double *dptr = (double *) multivalue;
  MRVector *y = (MRVector *) ptr;
  for (int k = 0; k < nvalues; k++) 
    sum += dptr[k];

  if (y) {
// printf("  kDDkDD rowsum %d %f\n", row, sum);
    y->AddEntry(row, sum);
  }
  else
    kv->add((char *) &row, sizeof(row), (char *) &sum, sizeof(sum));
}


/////////////////////////////////////////////////////////////////////////////
// MRMatrix::EmitEntries
// Emits all nonzero entries of matrix with key = row index.
// Flag add indicates whether to add entries to existing mr->kv (1) or to
// start a new mr->kv (0).
void MRMatrix::EmitEntries(
  MapReduce *mr,
  int add
)
{
  mr->map(mr->num_procs(), emit_matrix_entries, (void *)this, add);
}

/////////////////////////////////////////////////////////////////////////////
// emit_matrix_entries map() function
// For each non-zero in MRMatrix, emit (key,value) = (i, A_ij)
void emit_matrix_entries(int itask, KeyValue *kv, void *ptr)
{
  MRMatrix *A = (MRMatrix *) ptr;
  list<MatrixEntry>::iterator nz;
  for (nz=A->Amat.begin(); nz!=A->Amat.end(); nz++) {
//printf("DkkDkk EmitMatrix %d %f\n", (*nz).i, (*nz).nzv);
    kv->add((char *)&((*nz).i), sizeof((*nz).i), 
              (char *)&((*nz).nzv), sizeof((*nz).nzv));
  }
}

