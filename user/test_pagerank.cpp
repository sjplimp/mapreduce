// PageRank via MapReduce
// Karen Devine, 1416
// August 2008
//
// Performs PageRank on a square matrix A.
//
// Syntax: pagerank basefilename #_of_files N 
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
// The dimensions of the matrix A are given by N (N rows, N columns).
// Ideally, we would store this info in the files, but I haven't yet
// figured out how to do the broadcast necessary to get this info from
// the files to the processors.
//
// Values of the resulting vector y are written to stdout in sorted order:
//     row_i  y_i
//
// SVN Information:
//  $Author$
//  $Date$
//  $Revision$

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

#define ABS(a) ((a) >= 0. ? (a) : -1*(a));

//  MAP FUNCTIONS
MAPFUNCTION emit_vec_entries;

//  REDUCE FUNCTIONS
REDUCEFUNCTION compute_lmax_residual;
REDUCEFUNCTION output;

// COMPARISON FUNCTIONS
COMPAREFUNCTION compare;

////////////////////////////////////////////////////////////////////////////
// Global variables.

////////////////////////////////////////////////////////////////////////////
MRVector *pagerank(
  MapReduce *mr,
  MRMatrix *A,
  double alpha,
  double tolerance
)
{
  MRVector *x = new MRVector(mr, A->NumRows());
  MRVector *y = new MRVector(mr, x->GlobalLen());

  double randomlink = (1.-alpha)/(double)(x->GlobalLen());
  int iter, maxniter = (int) ceil(log10(tolerance) / log10(alpha));

  // Scale matrix A.
  A->Scale(alpha);
  x->PutScalar(1./x->GlobalLen());

  // Do all-zero row detection.


  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // PageRank iteration
  for (iter = 0; iter < maxniter; iter++) {

    // Compute adjustment for irreducibility (1-alpha)/n
    double ladj = 0.;
    double gadj = 0.;
    ladj = randomlink * x->LocalSum();

    // Compute local adjustment for all-zero rows.
    double allzeroadj = 0.;
    // NEED STUFF HERE
    ladj += allzeroadj;

    // Compute global adjustment via all-reduce-like operation.
    // Cheating here!  Should be done through MapReduce.
    MPI_Allreduce(&ladj, &gadj, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);

    // Compute global adjustment.
    A->MatVec(mr, x, y, 1);

    // Add adjustment to product vector in mr.
    y->AddScalar(gadj);

    // Compute max-norm of vector in mr.
    double gmax = y->GlobalMax(mr);

    // Scale vector in mr by 1/maxnorm.
    y->Scale(1./gmax);

    // Compute local max residual.
    double lresid = 0.;
    x->EmitEntries(mr, 0);
    y->EmitEntries(mr, 1);
    mr->collate(NULL);
    mr->reduce(compute_lmax_residual, &lresid);

    // Compute global max residual.
    // Cheating here!  Should be done through MapReduce.
    double gresid;
    MPI_Allreduce(&lresid, &gresid, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);


    //  Move result y to x for next iteration.
    MRVector *tmp = x;
    x = y;
    y = tmp;

    if (gresid < tolerance) 
      break;  // Iterations are done.
  }
  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();
  if (mr->my_proc() == 0)
    cout << " Number of iterations " << iter+1 << " Iteration Time "
         << tstop-tstart << endl;
  delete y;
  double gsum = x->GlobalSum(mr);
  x->Scale(1./gsum);
  return x;
}

////////////////////////////////////////////////////////////////////////////
// Reduce function:  compute_lmax_residual
// Multivalues received are the x and y values for a given matrix entry.
// Compute the difference and collect the max.
/////////////////////////////////////////////////////////////////////////////
// terms reduce() function
// input:  key = vector index j; multivalue = {x_j, y_j}
//         ptr = local max residual lresid.
// output: new max lresid
    
void compute_lmax_residual(char *key, int keylen, 
                           char *multivalue, int nvalues, int *mvlen,
                           KeyValue *kv, void *ptr)
{
  assert(nvalues == 2);
  double *lmax = (double *) ptr;
  double *values = (double *) multivalue;
  double diff = ABS(values[0] - values[1]);
  if (diff > *lmax) *lmax = diff;
}


////////////////////////////////////////////////////////////////////////////
int main(int narg, char **args)
{
  int me, np;      // Processor rank and number of processors.
  MPI_Init(&narg,&args);

  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&np);

  if (narg != 4) {
    if (me == 0) printf("Syntax: pagerank file.mtx numfiles N \n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
  int N = atoi(args[3]);  // Number of rows in matrix.
  int num_input_procs = atoi(args[2]);

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 0;

  // Persistent storage of the matrix. Will be loaded from files initially.
  MRMatrix A(mr, N, N, args[1], num_input_procs);

  // Call PageRank function.
  MRVector *x = pagerank(mr, &A, 0.8, 0.00001);  // Make alpha & tol input params later.

  // Output results:  Gather results to proc 0, sort and print.
  double xmin = x->GlobalMin(mr);    
  double xmax = x->GlobalMax(mr);       
  double xavg = x->GlobalSum(mr) / x->GlobalLen();
  if (x->GlobalLen() < 40) {
    if (me == 0) printf("PageRank Vector:\n");
    mr->map(np, &emit_vec_entries, x, 0);
    mr->gather(1);
    mr->sort_keys(&compare);
    mr->convert();
    mr->reduce(&output, NULL);
  }
  if (me == 0) {
    cout << "Page Rank Stats:  " << endl;
    cout << "      Max Value:  " << xmax << endl;
    cout << "      Min Value:  " << xmin << endl;
    cout << "      Avg Value:  " << xavg << endl;
  }

  // Clean up.
  delete mr;

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

