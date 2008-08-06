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
MAPFUNCTION load_matrix;
MAPFUNCTION emit_matrix;
MAPFUNCTION emit_vector;

//  REDUCE FUNCTIONS
REDUCEFUNCTION terms;
REDUCEFUNCTION rowsum;

/////////////////////////////////////////////////////////////////////////////
// Matrix constructor.  Reads matrix from files; stores in persistent memory.
// Does not emit anything, but uses Map to read the files.
MRMatrix::MRMatrix(
  MapReduce *mr,
  int n,          // Number of matrix rows 
  int m,          // Number of matrix columns 
  char *basefile,  // Base filename; NULL if want to re-use existing Amat.
  int numfiles     // Number of files to be read.
)
{
  LoadInfo f;
  f.A = this;
  f.basefile = basefile;


  // Read matrix-market files.
  int nnz = mr->map(numfiles, &load_matrix, (void *)&f, 0);
  if (mr->my_proc()==0) cout << "load_matrix Map Done:  nnz = " << nnz << endl;
}

/////////////////////////////////////////////////////////////////////////////
// matrix-vector multiplication function:  A * x.
// Assumes vector x is already in the MapReduce object mr with 
// (key,value) = (col index j, x_j);
// Matrix A is added to the map.

void MRMatrix::MatVec(
  MapReduce *mr,
  MRVector &x,
  bool transpose   // Flag indicating whether to do A*x or A^T * x.
)
{
  int me = mr->my_proc();
  int np = mr->num_procs();

  transposeFlag = transpose;

  // Emit matrix values.
  int nnz = mr->map(np, &emit_matrix, (void *)this, 0);
  if (me == 0) cout << "emit_matrix Map Done:  nnz = " << nnz << endl;

  // Emit vector values.
  int nv = mr->map(np, &emit_vector, (void *)&x, 1);
  if (me == 0) cout << "emit_vector Map Done:  nv = " << nv << endl;

  // Gather matrix column j and x_j to same processors.
  mr->collate(NULL);

  // Compute terms x_j * A_ij.
  int nterms = mr->reduce(&terms, NULL);
  if (me == 0) cout << "terms Reduce Done:  nterms = " << nterms << endl;

  // Gather matrix now by rows.
  mr->collate(NULL);

  // Compute sum of terms over rows.
  int nrow = mr->reduce(&rowsum, NULL);
  if (me == 0) cout << "rowsum Reduce Done:  nrow = " << nrow << endl;

  // Results y_i are in the MapReduce object mr.
}

/////////////////////////////////////////////////////////////////////////////
// emit_vector map() function
// For each entry in input MRVector, emit (key,value) = (j,[A_ij,i]).
// For transpose, emit (key,value) = (i,[A_ij,j]).
void emit_vector(int itask, KeyValue *kv, void *ptr)
{
  // Assume ptr = MRVector.
  MRVector *x = (MRVector *) ptr;
  INTDOUBLE value;

  int first = x->First();
  int len = x->Len();
  for (int i = first; i < first + len; i++) {
    value.i = XVECVALUE;
    value.d = (*x)[i];
    kv->add((char *)&i, sizeof(i), (char *) &value, sizeof(value));
  }
}

/////////////////////////////////////////////////////////////////////////////
// emit_matrix map() function
// For each non-zero in MRMatrix, emit (key,value) = (j,[A_ij,i]).
// For transpose, emit (key,value) = (i,[A_ij,j]).
void emit_matrix(int itask, KeyValue *kv, void *ptr)
{
  MRMatrix *A = (MRMatrix *) ptr;
  bool transpose = A->UseTranspose();

  list<MatrixEntry>::iterator nz;
  for (nz=A->Amat.begin(); nz!=A->Amat.end(); nz++) {
    INTDOUBLE value;
    value.d = (*nz).nzv;
    if (transpose) {
      value.i = (*nz).j;
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

// load_matrix map() function
// Read matrix from matrix-market file.
// Assume matrix-market file is split into itask chunks.
// To allow re-use of the matrix without re-reading the files, 
// store the nonzeros read by this processor.

void load_matrix(int itask, KeyValue *kv, void *ptr)
{
  LoadInfo *fptr = (LoadInfo *) ptr;
  MRMatrix *A = fptr->A;

  A->MakeEmpty();

  // Read the matrix from a file.
  char filename[81];
  sprintf(filename, "%s.%04d", fptr->basefile, itask); // File for this task.
  FILE *fp = fopen(filename,"r");
  if (fp == NULL) {
    cout << "File not found:  " << filename << endl;
    exit(-1);
  }
  
  int i, j;
  double nzv;
  // Read matrix market file.  Emit (key, value) = (j, [A_ij,i]).
  while (fscanf(fp, "%d %d %lf", &i, &j, &nzv) == 3)  {
    A->AddNonzero(i, j, nzv);
  }
 
  fclose(fp);
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
    kv->add((char *) &aptr->i, sizeof(aptr->i), 
            (char *) &product, sizeof(product));
  }
}

/////////////////////////////////////////////////////////////////////////////
// rowsum reduce() function
// input:  key = row index i; 
//         multivalue = {x_j*A_ij for all j with nonzero A_ij}.
// output: key = row index i; value = sum over j [x_j * A_ij] = y_i.
void rowsum(char *key, int keylen, char *multivalue, int nvalues, int *mvlen,
            KeyValue *kv, void *ptr)
{
  double sum = 0;
  int row = *(int*) key;
  double *dptr = (double *) multivalue;
  for (int k = 0; k < nvalues; k++) 
    sum += dptr[k];
  kv->add((char *) &row, sizeof(row), (char *) &sum, sizeof(sum));
}

