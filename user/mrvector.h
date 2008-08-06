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

using namespace MAPREDUCE_NS;
using namespace std;

#include "mrall.h"

/////////////////////////////////////////////////////////////////////////////
class MRVector {
  public:
    MRVector(MapReduce *mr, int n);
    ~MRVector() {delete [] vec;}
    double operator[] (int global_i) {
      return vec[global_i-first];
    };
    int First() { return first; };
    int Len() { return local_len; };
    int GlobalLen() { return global_len; };
  private:
    int global_len;  // Global number of entries in vector
    int first;       // First global entry in vector; one-based.
    int local_len;   // Number of local entries in vector.
    double *vec;
};
