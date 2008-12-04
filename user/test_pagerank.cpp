// PageRank via MapReduce
// Karen Devine, 1416
// August 2008
//
// Performs PageRank on a square matrix A using MapReduce library.
//
// Syntax: pagerank basefilename N [store_by_map 0/1]
//
// Assumes matrix file format as follows:
//     row_i  col_j  nonzero_value    (one line for each local nonzero)
// The number of these files is given by the #_of_files argument
// on the command line.  These files will be read in parallel if
// #_of_files > 1.
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
#include <assert.h>
#include <unistd.h>

using namespace MAPREDUCE_NS;
using namespace std;

#define ABS(a) ((a) >= 0. ? (a) : -1*(a));


//  MAP FUNCTIONS
MAPFUNCTION emit_allzero_rows;
MAPFUNCTION emit_matvec_empty_terms;

//  REDUCE FUNCTIONS
REDUCEFUNCTION compute_lmax_residual;
REDUCEFUNCTION collect_allzero_rows;
REDUCEFUNCTION allzero_contribution;
REDUCEFUNCTION output;

// COMPARISON FUNCTIONS
COMPAREFUNCTION compare;

////////////////////////////////////////////////////////////////////////////
// Global variables.

////////////////////////////////////////////////////////////////////////////
void detect_allzero_rows(
  MapReduce *mr,
  MRMatrix *A,
  list<int> *allzero   // Output:  Indices of allzero rows in A.
)
{
  // Emit nonzeros of matrix.
  A->EmitEntries(mr, 0);
  // Emit empty terms (row i, 0).
  mr->map(A->NumRows(), &emit_matvec_empty_terms, NULL, 1);

  mr->collate(NULL);

  mr->reduce(&collect_allzero_rows, (void *) allzero);
}

////////////////////////////////////////////////////////////////////////////
// collect_allzero_rows reduce() function
// Input:  matrix entries (i, a_ij) + unit vector entries (i, e_i).
// Output:  Updated allzero list of rows that are allzero.
void collect_allzero_rows(char *key, int keylen, char *multivalue, 
                          int nvalues, int *mvlen,
                          KeyValue *kv, void *ptr)
{
  list<int> *allzero = (list<int> *) ptr;
  if (nvalues == 1) {
    // only the identity vector entry existed for this key -- no a_ij values!
    int row = *((int*) key);
    allzero->push_front(row);
  }
}

////////////////////////////////////////////////////////////////////////////
// Compute local contribution to adjustment for allzero rows.
double compute_local_allzero_adj(
  MapReduce *mr,
  MRVector *x,
  list<int> *allzero,
  double alpha,
  bool storage_aware
)
{
  double sum = 0.;

  mr->map(mr->num_procs(), emit_allzero_rows, allzero, 0);
  x->EmitEntries(mr, 1);
  mr->collate(NULL);  // this should require little or no communication.
  mr->reduce(allzero_contribution, &sum);

  return (alpha * sum / x->GlobalLen());
}

////////////////////////////////////////////////////////////////////////////
// emit_allzero_rows  map() function
// Emit (i, 0) for allzero row i.
void emit_allzero_rows(int itask, KeyValue *kv, void *ptr)
{
  list<int> *allzero = (list<int> *) ptr;
  list<int>::iterator i;
  double zero = 0.;
  for (i = allzero->begin(); i != allzero->end(); i++)
    kv->add((char *) &(*i), sizeof(*i), (char *) &zero, sizeof(zero));
}

////////////////////////////////////////////////////////////////////////////
// allzero_contribution reduce() function
// Input:  vector entries (i, v_i) + indices of allzero rows (i, 0)
// Output:  sum of v_j for each allzero row j; stored in ptr.
void allzero_contribution(char *key, int keylen, char *multivalue, 
                          int nvalues, int *mvlen,
                          KeyValue *kv, void *ptr)
{
  if (nvalues > 1) {
    // This is an allzero row; multivalue as both allzero row index and x_j.
    double *sum = (double *) ptr;
    double *values = (double *) multivalue;
    for (int i = 0; i < nvalues; i++) 
      *sum += values[i];
  }
}

////////////////////////////////////////////////////////////////////////////
MRVector *pagerank(
  MapReduce *mr,
  MRMatrix *A,
  double alpha,
  double tolerance,
  int storage_format,
  bool storage_aware
)
{
  int me = mr->my_proc();
  if (me == 0) {cout << "Initializing vectors..." << endl; flush(cout);}
  MRVector *x = new MRVector(mr, A->NumRows(), storage_format);
  MRVector *y = new MRVector(mr, x->GlobalLen(), storage_format);

  double randomlink = (1.-alpha)/(double)(x->GlobalLen());
  int iter; 
  int maxniter = (int) ceil(log10(tolerance) / log10(alpha));

  // Scale matrix A.
  A->Scale(alpha);
  x->PutScalar(1./x->GlobalLen());

  // Do all-zero row detection.
  if (me == 0) {cout << "Detecting allzero rows..." << endl; flush(cout);}
  list<int> allzero;
  detect_allzero_rows(mr, A, &allzero);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

#ifdef KDDTIME
double KDDmatvec = 0.;
double KDDallzero = 0.;
double KDDadjust = 0.;
double KDDresid = 0.;
double KDDmaxmatvec = 0.;
double KDDmaxallzero = 0.;
double KDDmaxadjust = 0.;
double KDDmaxresid = 0.;
double KDDminmatvec = 0.;
double KDDminallzero = 0.;
double KDDminadjust = 0.;
double KDDminresid = 0.;
double KDDtmp;
#endif //KDDTIME

  if (me == 0) {cout << "Beginning iterations..." << endl; flush(cout);}
  // PageRank iteration
  for (iter = 0; iter < maxniter; iter++) {

#ifdef KDDTIME
MPI_Barrier(MPI_COMM_WORLD);
KDDtmp = MPI_Wtime();
#endif //KDDTIME

    // Compute adjustment for irreducibility (1-alpha)/n
    double ladj = 0.;
    double gadj = 0.;
    ladj = randomlink * x->LocalSum();

    // Compute local adjustment for all-zero rows.
    double allzeroadj = 0.;
    allzeroadj = compute_local_allzero_adj(mr, x, &allzero, alpha, 
                                           storage_aware);
    ladj += allzeroadj;

    // Compute global adjustment via all-reduce-like operation.
    // Cheating here!  Should be done through MapReduce.
    MPI_Allreduce(&ladj, &gadj, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);

#ifdef KDDTIME
KDDallzero += (MPI_Wtime() - KDDtmp);
MPI_Barrier(MPI_COMM_WORLD);
KDDtmp = MPI_Wtime();
#endif //KDDTIME

    // Compute global adjustment.
    A->MatVec(mr, x, y, 1, storage_aware);

#ifdef KDDTIME
KDDmatvec += (MPI_Wtime() - KDDtmp);
MPI_Barrier(MPI_COMM_WORLD);
KDDtmp = MPI_Wtime();
#endif //KDDTIME

    // Add adjustment to product vector in mr.
    y->AddScalar(gadj);

    // Compute max-norm of vector in mr.
    double gmax = y->GlobalMax(mr);

    // Scale vector in mr by 1/maxnorm.
    y->Scale(1./gmax);

#ifdef KDDTIME
KDDadjust += (MPI_Wtime() - KDDtmp);
MPI_Barrier(MPI_COMM_WORLD);
KDDtmp = MPI_Wtime();
#endif //KDDTIME

    // Compute local max residual.
    double lresid = 0.;
    if (storage_aware) {
      y->vec.sort();
      list<INTDOUBLE>::iterator v, w;
      for (v=x->vec.begin(), w=y->vec.begin(); v!=x->vec.end(); v++, w++) {
        // Sanity check 
        if ((*v).i != (*w).i) cout << "I HATE LIFE\n" << endl;
        double tmp = ABS((*v).d - (*w).d);
        lresid = (tmp > lresid ? tmp : lresid);
      }
    }
    else {
      x->EmitEntries(mr, 0);
      y->EmitEntries(mr, 1);
      mr->collate(NULL);
      mr->reduce(compute_lmax_residual, &lresid);
    }

    // Compute global max residual.
    // Cheating here!  Should be done through MapReduce.
    double gresid;
    MPI_Allreduce(&lresid, &gresid, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);

#ifdef KDDTIME
KDDresid += (MPI_Wtime() - KDDtmp);
#endif //KDDTIME

    //  Move result y to x for next iteration.
    MRVector *tmp = x;
    x = y;
    y = tmp;

    // if (me == 0) {
    //   cout << "iteration " << iter+1 << " resid " << gresid << endl; 
    //   flush(cout);
    // }

    if (gresid < tolerance) 
      break;  // Iterations are done.
  }
  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

#ifdef KDDTIME
MPI_Allreduce(&KDDallzero, &KDDmaxallzero, 1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
MPI_Allreduce(&KDDmatvec,  &KDDmaxmatvec,  1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
MPI_Allreduce(&KDDadjust,  &KDDmaxadjust,  1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
MPI_Allreduce(&KDDresid,   &KDDmaxresid,   1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
MPI_Allreduce(&KDDallzero, &KDDminallzero, 1,MPI_DOUBLE,MPI_MIN,MPI_COMM_WORLD);
MPI_Allreduce(&KDDmatvec,  &KDDminmatvec,  1,MPI_DOUBLE,MPI_MIN,MPI_COMM_WORLD);
MPI_Allreduce(&KDDadjust,  &KDDminadjust,  1,MPI_DOUBLE,MPI_MIN,MPI_COMM_WORLD);
MPI_Allreduce(&KDDresid,   &KDDminresid,   1,MPI_DOUBLE,MPI_MIN,MPI_COMM_WORLD);
if (mr->my_proc() == 0) cout << "ALLZERO: " << KDDminallzero 
                             << " " << KDDmaxallzero << "\n" 
                             << "MATVEC:  " << KDDminmatvec  
                             << " " << KDDmaxmatvec  << "\n"
                             << "ADJUST:  " << KDDminadjust  
                             << " " << KDDmaxadjust  << "\n" 
                             << "RESID:   " << KDDminresid   
                             << " " << KDDmaxresid   << "\n";
#endif //KDDTIME


  if (mr->my_proc() == 0)
    cout << " Number of iterations " << iter+1 << " Iteration Time "
         << tstop-tstart << endl;
  delete y;
  double gsum = x->GlobalSum(mr);
  x->Scale(1./gsum);
  A->Scale(1./alpha);  // Return A to original condition
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
// Print some simple stats.
void simple_stats(MapReduce *mr, MRMatrix *A, MRVector *x)
{
int me = mr->my_proc();
int np;  MPI_Comm_size(MPI_COMM_WORLD, &np);
int lnentry, maxnentry, minnentry, sumnentry;

  lnentry = A->Amat.size();
  MPI_Allreduce(&lnentry, &maxnentry, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
  MPI_Allreduce(&lnentry, &minnentry, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
  MPI_Allreduce(&lnentry, &sumnentry, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
  if (me == 0) 
    printf("Matrix Stats:  nonzeros/proc (max, min, avg):  %d %d %d\n", 
           maxnentry, minnentry, sumnentry/np);

  lnentry = x->vec.size();
  MPI_Allreduce(&lnentry, &maxnentry, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
  MPI_Allreduce(&lnentry, &minnentry, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
  MPI_Allreduce(&lnentry, &sumnentry, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
  if (me == 0) 
    printf("Vector Stats:  entries/proc (max, min, avg):  %d %d %d\n", 
           maxnentry, minnentry, sumnentry/np);
}

////////////////////////////////////////////////////////////////////////////
int main(int narg, char **args)
{
  int me, np;      // Processor rank and number of processors.
  MPI_Init(&narg,&args);

  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&np);
  if (me == 0) {cout << "Here we go..." << endl; flush(cout);}

  // Default parameters
  bool storage_aware = 0;
  bool store_by_map = 0;
  double alpha = 0.8;
  double tolerance = 0.00001;
  int NumberOfPageranks = 1;

  // Parse the command line.
  int ch;
  opterr = 0;
  char *optstring = "a:t:n:g:s:";

  while ((ch = getopt(narg, args, optstring)) != -1) {
    switch (ch) {
    case 'a':
      // Pagerank relaxation param
      alpha = atof(optarg);
      break;
    case 't': 
      // Pagerank tolerance
      tolerance = atof(optarg);
      break;
    case 'n':
      // Number of times to do pagerank
      NumberOfPageranks = atoi(optarg);
      break;
    case 'g':
      // Algorithm to use:  pure_mapreduce or storage_aware
      if (optarg[0] == 'p' || optarg[0] == 'P') 
        storage_aware = 0;
      else if (optarg[0] == 's' || optarg[0] == 'S') 
        storage_aware = 1;
      else {
        cout << "Invalid value for option -g" << endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
      }
      break;
    case 's':
      // Storage to use:  FILE or MAP
      if (optarg[0] == 'f' || optarg[0] == 'F') 
        store_by_map = 0;
      else if (optarg[0] == 'm' || optarg[0] == 'M') 
        store_by_map = 1;
      else {
        cout << "Invalid value for option -s" << endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
      }
      break;
    case '?':
      printf("Invalid option -%c\n", optopt);
      MPI_Abort(MPI_COMM_WORLD, -1);
      break;
    }
  }

 
  if ((narg-optind) < 2) {
    if (me == 0) 
      cout << "Syntax: pagerank [-s {FILE|MAP}] "
           << "[-g {PURE_MAPREDUCE|STORAGE_AWARE}] [-a alpha] [-t tolerance] "
           << "[-n NumberOfPageranks] file.mtx N" << endl;
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  int N = atoi(args[optind+1]);  // Number of rows in matrix.
  int storage_format = (store_by_map ? BY_ROW : BY_FILE);  // Best to store by
                                                           // row for pagerank
  if (!store_by_map && storage_aware) {
    cout << "Invalid options; must use store_by_map with "
         << "storage_aware algorithm" << endl;
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 0;
  mr->mapstyle = 1;  // mapstyle == 0 does not work for this code.

  // Persistent storage of the matrix. Will be loaded from files initially.
  if (me == 0) {cout << "Loading matrix..." << endl; flush(cout);}
  MRMatrix A(mr, N, N, args[optind], store_by_map, storage_aware);

  // Call PageRank function.
  for (int npr = 0; npr < NumberOfPageranks; npr++) {
    if (me == 0) {cout << "Calling pagerank..." << endl; flush(cout);}
    MRVector *x = pagerank(mr, &A, alpha, tolerance,
                           storage_format, storage_aware);  
    if (me == 0) {cout << "Pagerank done..." << endl; flush(cout);}

    // Output results:  Gather results to proc 0, sort and print.
    double xmin = x->GlobalMin(mr);    
    double xmax = x->GlobalMax(mr);       
    double xavg = x->GlobalSum(mr) / x->GlobalLen();
    if (x->GlobalLen() < 40) {
      if (me == 0) printf("PageRank Vector:\n");
      x->EmitEntries(mr, 0);
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
    simple_stats(mr, &A, x);
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

