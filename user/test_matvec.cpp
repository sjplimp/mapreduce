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
MAPFUNCTION initialize_xvec;

//  REDUCE FUNCTIONS
REDUCEFUNCTION output;

// COMPARISON FUNCTIONS
COMPAREFUNCTION compare;

////////////////////////////////////////////////////////////////////////////
// Global variables.

////////////////////////////////////////////////////////////////////////////
int main(int narg, char **args)
{
  int Me, Np;      // Processor rank and number of processors.
  MPI_Init(&narg,&args);

  MPI_Comm_rank(MPI_COMM_WORLD,&Me);
  MPI_Comm_size(MPI_COMM_WORLD,&Np);

  if (narg != 5) {
    if (Me == 0) printf("Syntax: matvec file.mtx numfiles N M\n");
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
  list<MatrixEntry> Amat;

  //  **********************   A * x   ************************
  // Initialize x vector.
  int xcol = mr->map(M, &initialize_xvec, &M, 0);
  if (Me == 0) cout << "initialize_xvec Map Done:  xcol = " << xcol << endl;

  // Perform matrix-vector multiplication.
  matvec(mr, &Amat, 0, args[1], num_input_procs);

  // Output results:  Gather results to proc 0, sort and print.
  int nkeys = mr->gather(1);
  nkeys = mr->sort_keys(&compare);
  
  // Print results.
  nkeys = mr->convert();
  nkeys = mr->reduce(&output, NULL);
  if (Me == 0) cout << "Output done:  nkeys = " << nkeys << endl;

  //  **********************   A^T * x   ************************
  // Initialize x vector.
  int xrow = mr->map(N, &initialize_xvec, &N, 0);
  if (Me == 0) cout << "initialize_xvec Map Done:  xrow = " << xrow << endl;

  // Perform matrix-vector multiplication with transpose; re-use A.
  matvec(mr, &Amat, 1, NULL, num_input_procs);

  // Output results:  Gather results to proc 0, sort and print.
  nkeys = mr->gather(1);
  nkeys = mr->sort_keys(&compare);
  
  // Print results.
  nkeys = mr->convert();
  nkeys = mr->reduce(&output, NULL);
  if (Me == 0) cout << "TRANSPOSE Output done:  nkeys = " << nkeys << endl;

  // Clean up.
  delete mr;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (Me == 0) {
    printf("Time to matvec %s on %d procs = %g (secs)\n",
	   args[1], Np, tstop-tstart);
  }

  MPI_Finalize();
}

/////////////////////////////////////////////////////////////////////////////
// initialize_xvec map() function
// For each vector entry x_j in vector x, emit (j, x_j).
// Note:  Include flag in INTDOUBLE indicating that this emission is
// from vector x, not column j.

void initialize_xvec(int itask, KeyValue *kv, void *ptr)
{
// Emit uniform input vector.
// Assume ptr = number of columns in matrix = M = ncol.
// Assume initialize_xvec is issued once for each column.
  int ncol = *(int *)ptr;
  INTDOUBLE value;

  value.i = XVECVALUE;
  value.d = 1. / (float) ncol; // Uniform x_j.

  itask++;  // Matrix-market is one-based; the vector should be, too.
  kv->add((char *)&itask, sizeof(itask), (char *) &value, sizeof(value));
}

/////////////////////////////////////////////////////////////////////////////
// output reduce() function
// input:  key = row index i; 
//         multivalue = {x_j*A_ij for all j with nonzero A_ij}.
// output: print (i, y_i).

void output(char *key, int nvalues, char **multivalue, KeyValue *kv, void *ptr)
{
  assert(nvalues == 1);
  cout << *(int*) key <<  "    " << *(double*)multivalue[0] << endl;
}

/////////////////////////////////////////////////////////////////////////////
// compare comparison function.
// Compares two integer keys a and b; returns -1, 0, 1 if a<b, a==b, a>b,
// respectively.
int compare(char *a, char *b)
{
int ia = *(int*)a;
int ib = *(int*)b;
  if (ia < ib) return -1;
  if (ia > ib) return  1;
  return 0;
}

