// MatVec via MapReduce
// Karen Devine, 1416
// June 2008
//
// Performs matrix-vector multiplication  A*x=y.
//
// Syntax: matvec basefilename #_of_files N M
//
// Assumes matrix file format as follows:
//     row_i  col_j  nonzero_value    (one line for each local nonzero)
// The number of these files is given by the #_of_files argument
// on the command line.  These files will be read in parallel if
// #_of_files > 1.
//
// The files should be named basefilename.0000, basefilename.0001, etc.
// up to the #_of_files-1.
//
// The dimensions of the matrix A are given by N and M (N rows, M columns).
// Ideally, we would store this info in the files, but I haven't yet
// figured out how to do the broadcast necessary to get this info from
// the files to the processors.
//
// Values of the resulting vector y are written to stdout in sorted order:
//     row_i  y_i
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

void matvec(
  MapReduce *mr,
  list<MatrixEntry> *Amat,
  char *basefile,
  int numfiles
)
{
  FileInfo files;
  files.Amatptr = Amat;
  files.basefile = basefile;

  // Read matrix-market files.
  int nnz = mr->map(numfiles, &load_matrix, (void *)&files, 1);
  cout << "First Map Done:  nnz = " << nnz << endl;

  // Gather matrix column j and x_j to same processors.
  mr->collate(NULL);

  // Compute terms x_j * A_ij.
  int nterms = mr->reduce(&terms, NULL);
  cout << "Second Reduce Done:  nterms = " << nterms << endl;

  // Gather matrix now by rows.
  mr->collate(NULL);

  // Compute sum of terms over rows.
  int nrow = mr->reduce(&rowsum, NULL);
  cout << "Third Reduce Done:  nrow = " << nrow << endl;

  // Results y_i are in the MapReduce object mr.
}

/////////////////////////////////////////////////////////////////////////////

// load_matrix map() function
// For each non-zero in matrix-market file, emit (key,value) = (j,[A_ij,i]).
// Assume matrix-market file is split into itask chunks.
// To allow re-use of the matrix without re-reading the files, 
// store the nonzeros and just re-emit them to re-use the matrix.

void load_matrix(int itask, KeyValue *kv, void *ptr)
{
  FileInfo *fptr = (FileInfo *) ptr;
  list<MatrixEntry> *Amat = fptr->Amatptr;

  if (fptr->basefile == NULL) {
    // No file name provided; assume previously read the matrix into Amat.
    list<MatrixEntry>::iterator nz;
    for (nz=Amat->begin(); nz!=Amat->end(); nz++) {
      kv->add((char *)&((*nz).j), sizeof((*nz).j), 
              (char *)&((*nz).value), sizeof((*nz).value));
    }
  }
  else {
    // First, clear Amat if not already clear.
    Amat->clear();

    // Read the matrix from a file.
    char filename[81];
    sprintf(filename, "%s.%04d", fptr->basefile, itask); // File for this task.
    FILE *fp = fopen(filename,"r");
    if (fp == NULL) {
      cout << "File not found:  " << filename << endl;
      exit(-1);
    }
    
    // Values emitted are pair [A_ij,i].
    struct MatrixEntry nz;
  
    // Read matrix market file.  Emit (key, value) = (j, [A_ij,i]).
    while (fscanf(fp, "%d %d %lf", &nz.value.i, &nz.j, &nz.value.d) == 3)  {
      Amat->push_front(nz);
      kv->add((char *)&(nz.j), sizeof(nz.j),
              (char *)&(nz.value), sizeof(nz.value));
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

