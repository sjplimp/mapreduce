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
#include <math.h>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "mrmatrix.h"
#include "mrvector.h"

using namespace MAPREDUCE_NS;
using namespace std;

//  REDUCE FUNCTIONS
REDUCEFUNCTION output;

// COMPARISON FUNCTIONS
COMPAREFUNCTION compare;

////////////////////////////////////////////////////////////////////////////
// Global variables.

////////////////////////////////////////////////////////////////////////////
void pagerank(
  MapReduce *mr,
  MRMatrix &A,
  MRVector &x,
  double alpha,
  double tolerance
)
{
  int maxniter = (int) ceil(log10(tolerance) / log10(alpha));

  // Scale matrix A.
  A.Scale(alpha);
  x.PutScalar(1./x.GlobalLen());

  // Do all-zero row detection.

  double randomlink = (1.-alpha)/(double)x.GlobalLen();
  int first = x.First();
  int len = x.Len();

  for (int iter = 0; iter < maxniter; iter++) {

    // Compute adjustment for irreducibility (1-alpha)/n
    double ladj = 0.;
    for (int i = first; i < first + len; i++) 
      ladj += x[i];
    ladj *= randomlink;

    // Compute local adjustment for all-zero rows.

    // Compute global adjustment via all-reduce-like operation.

    // Compute global adjustment.
    A.MatVec(mr, x, 1);

    // Add adjustment to product vector in mr.
    // That is a map operation, I think; for each key-value pair, issue
    // key, value+adjustment pair.

    // Compute max-norm of vector in mr.

    // Scale vector in mr by 1/maxnorm.

    // Compute local max residual.

    // Compute global max residual.
    double residual;

    if (residual < tolerance) 
      break;  // Iterations are done.
  }
}

////////////////////////////////////////////////////////////////////////////
int main(int narg, char **args)
{
  int me, np;      // Processor rank and number of processors.
  MPI_Init(&narg,&args);

  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&np);

  if (narg != 5) {
    if (me == 0) printf("Syntax: matvec file.mtx numfiles N M\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
  int N = atoi(args[3]);  // Number of rows in matrix.
  int M = atoi(args[4]);  // Number of cols in matrix.
  int num_input_procs = atoi(args[2]);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 0;

  // Persistent storage of the matrix. Will be loaded from files initially.
  MRMatrix A(mr, N, M, args[1], num_input_procs);

  // Allocate and initialize persistent storage for pagerank vector.
  MRVector x(mr, M);

  // Call PageRank function.
  pagerank(mr, A, x, 0.8, 0.00001);  // Make alpha & tol input params later.

  // Output results:  Gather results to proc 0, sort and print.
  int nkeys = mr->gather(1);
  nkeys = mr->sort_keys(&compare);
  
  // Print results.
  nkeys = mr->convert();
  nkeys = mr->reduce(&output, NULL);
  if (me == 0) cout << "Output done:  nkeys = " << nkeys << endl;

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

