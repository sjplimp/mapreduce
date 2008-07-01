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
#include "matvec.h"

using namespace MAPREDUCE_NS;
using namespace std;

//  MAP FUNCTIONS
MAPFUNCTION load_matrix;

//  REDUCE FUNCTIONS
REDUCEFUNCTION terms;
REDUCEFUNCTION rowsum;

/////////////////////////////////////////////////////////////////////////////

// matrix-vector multiplication function:  A * x.
// Assumes vector x is already in the MapReduce object mr with 
// (key,value) = (col index j, x_j);
// Matrix A is added to the map.

void MRMatrix::MatVec(
  MapReduce *mr,
  bool transpose,  // Flag indicating whether to do A*x or A^T * x.
  char *basefile,  // Base filename; NULL if want to re-use existing Amat.
  int numfiles     // Number of files to be read.
)
{
  LoadInfo f;
  f.A = this;
  f.basefile = basefile;
  f.transpose = transpose;

  int Me;
  MPI_Comm_rank(MPI_COMM_WORLD, &Me);

  // Read matrix-market files.
  int nnz = mr->map(numfiles, &load_matrix, (void *)&f, 1);
  if (Me == 0) cout << "load_matrix Map Done:  nnz = " << nnz << endl;

  // Gather matrix column j and x_j to same processors.
  mr->collate(NULL);

  // Compute terms x_j * A_ij.
  int nterms = mr->reduce(&terms, NULL);
  if (Me == 0) cout << "terms Reduce Done:  nterms = " << nterms << endl;

  // Gather matrix now by rows.
  mr->collate(NULL);

  // Compute sum of terms over rows.
  int nrow = mr->reduce(&rowsum, NULL);
  if (Me == 0) cout << "rowsum Reduce Done:  nrow = " << nrow << endl;

  // Results y_i are in the MapReduce object mr.
}

/////////////////////////////////////////////////////////////////////////////

// load_matrix map() function
// For each non-zero in matrix-market file, emit (key,value) = (j,[A_ij,i]).
// For transpose, emit (key,value) = (i,[A_ij,j]).
// Assume matrix-market file is split into itask chunks.
// To allow re-use of the matrix without re-reading the files, 
// store the nonzeros and just re-emit them to re-use the matrix.

void load_matrix(int itask, KeyValue *kv, void *ptr)
{
  LoadInfo *fptr = (LoadInfo *) ptr;
  MRMatrix *A = fptr->A;
  bool transpose = fptr->transpose;

  if (fptr->basefile == NULL) {
    // No file name provided; assume previously read the matrix into Amat.
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
  else {
    // First, clear Amat if not already clear.
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
      INTDOUBLE value;
      value.d = nzv;
      if (transpose) {
        value.i = j;
        kv->add((char *)&i, sizeof(i),
                (char *)&value, sizeof(value));
      }
      else {
        value.i = i;
        kv->add((char *)&j, sizeof(j),
                (char *)&value, sizeof(value));
      }
    }
  
    fclose(fp);
  }
}

/////////////////////////////////////////////////////////////////////////////
// terms reduce() function
// input:  key = column index; multivalue = {x_j, of A_ij for
//         all i with nonzero A_ij.}
// output:  key = row index i; value = x_j * A_ij.

void terms(char *key, int nvalues, char **multivalue, KeyValue *kv, void *ptr)
{
INTDOUBLE *xptr;
INTDOUBLE *aptr;

  if (nvalues == 1) 
    // No nonzeros in this column; skip it.
    return;

  // Find the x_j value
  for (int k = 0; k < nvalues; k++) {
    xptr = (INTDOUBLE *) multivalue[k];
    if (xptr->i < 0)  // found it
      break;
  }
  double x_j = xptr->d;

  for (int k = 0; k < nvalues; k++) {
    aptr = (INTDOUBLE *) multivalue[k];
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
void rowsum(char *key, int nvalues, char **multivalue, KeyValue *kv, void *ptr)
{
  double sum = 0;
  int row = *(int*) key;
  for (int k = 0; k < nvalues; k++) 
    sum += *(double *) multivalue[k];
  kv->add((char *) &row, sizeof(row), (char *) &sum, sizeof(sum));
}

