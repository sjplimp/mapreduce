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
class MRVector;

////////////////////////////////////////////////////////////////////////////

typedef struct MatrixEntry {
  int i;            // row index
  int j;            // column index
  double nzv;       // non-zero value.
};

class MRMatrix {
  public:  
    MRMatrix(MapReduce *, int, int, char *, bool store_by_map=0);
    ~MRMatrix() {Amat.clear();}

    int NumRows() { return N; }
    int NumCols() { return M; }
    void AddNonzero(int i, int j, double nzv) {
      struct MatrixEntry nz;
      nz.i = i;
      nz.j = j;
      nz.nzv = nzv;
// printf("HOHO AddNonzero %d %d %f\n", i, j, nzv);
      Amat.push_front(nz);
    };
    void MatVec(MapReduce *, MRVector *, MRVector *, bool);
    void Scale(double);
    void MakeEmpty() {Amat.clear();};
    bool UseTranspose() {return transposeFlag;}
    void EmitEntries(MapReduce *, int);
    list<MatrixEntry> Amat;  // Non-zeros; probably should make private later.
  private:
    int N;  // Number of rows
    int M;  // Number of cols
    bool transposeFlag;  // State variable; indicates whether to use 
                         // A or A^T in current operation.
};

