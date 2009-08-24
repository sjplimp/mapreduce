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
#include "mrmatrix.h"
#include "mrvector.h"
#include <assert.h>

using namespace MAPREDUCE_NS;
using namespace std;

//  REDUCE FUNCTIONS
REDUCEFUNCTION output;

// COMPARISON FUNCTIONS
COMPAREFUNCTION compare;

////////////////////////////////////////////////////////////////////////////
// Global variables.

////////////////////////////////////////////////////////////////////////////
int main(int narg, char **args)
{
  int me, np;      // Processor rank and number of processors.
  MPI_Init(&narg,&args);

  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&np);

  if (narg != 4) {
    if (me == 0) printf("Syntax: matvec file.mtx numfiles N M\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
  int N = atoi(args[2]);  // Number of rows in matrix.
  int M = atoi(args[3]);  // Number of cols in matrix.

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 0;

  // Persistent storage of the matrix. Will be loaded from files initially.
  MRMatrix A(mr, N, M, args[1]);

  //  **********************   A * x   ************************
  // Allocate and initialize persistent storage for vector.
  MRVector x(mr, M);
  x.PutScalar(1./(double)M);

  // Perform matrix-vector multiplication.
  A.MatVec(mr, &x, NULL, 0);

  // Output results:  Gather results to proc 0, sort and print.
  int nkeys = mr->gather(1);
  nkeys = mr->sort_keys(&compare);
  
  // Print results.
  nkeys = mr->convert();
  nkeys = mr->reduce(&output, NULL);
  if (me == 0) cout << "Output done:  nkeys = " << nkeys << endl;

  //  **********************   A^T * x   ************************
  // Allocate and initialize persistent storage for vector.
  MRVector xt(mr, N);
  xt.PutScalar(1./(double)N);

  // Perform matrix-vector multiplication with transpose; re-use A.
  A.MatVec(mr, &xt, NULL, 1);

  // Output results:  Gather results to proc 0, sort and print.
  nkeys = mr->gather(1);
  nkeys = mr->sort_keys(&compare);
  
  // Print results.
  nkeys = mr->convert();
  nkeys = mr->reduce(&output, NULL);
  if (me == 0) cout << "TRANSPOSE Output done:  nkeys = " << nkeys << endl;

  // Clean up.
  delete mr;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0) {
    printf("Time to matvec %s on %d procs = %g (secs)\n",
	   args[1], np, tstop-tstart);
  }

  MPI_Finalize();
}

/////////////////////////////////////////////////////////////////////////////
// output reduce() function
// input:  key = row index i; 
//         multivalue = {x_j*A_ij for all j with nonzero A_ij}.
// output: print (i, y_i).

void output(char *key, int keylen, char *multivalue, int nvalues, int *mvlen, 
            KeyValue *kv, void *ptr)
{
  assert(nvalues == 1);
  double *dptr = (double *) multivalue;
  cout << *(int*) key <<  "    " << dptr[0] << endl;
}

/////////////////////////////////////////////////////////////////////////////
// compare comparison function.
// Compares two integer keys a and b; returns -1, 0, 1 if a<b, a==b, a>b,
// respectively.
int compare(char *a, int lena, char *b, int lenb)
{
int ia = *(int*)a;
int ib = *(int*)b;
  if (ia < ib) return -1;
  if (ia > ib) return  1;
  return 0;
}

