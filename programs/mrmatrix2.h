// MapReduce Matrix class
// Karen Devine, 1416
// April 2010
//
// Store matrix as a MapReduce object, with different storage depending on
// whether matrix A is stored or transpose A^T is stored.
// For A:  (Key, Value) = (row index i, (col index j, a_ij))
// For A^T:  (Key, Value) = (col index j, (row index i, a_ij))
// 
// Provides method for matrix-vector multiplication  A*x=y.
//
#include <iostream>
#include <list>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"

using namespace MAPREDUCE_NS;
using namespace std;

#include "mrall2.h"
#include "mrvector2.h"

#ifndef __MRMATRIX
#define __MRMATRIX

////////////////////////////////////////////////////////////////////////////

template <typename IDTYPE>
class MRNonZero {
public:
  IDTYPE ij;        // column (if A) or row (if A^T) index
  double nzv;       // non-zero value.
  MRNonZero() {};
  ~MRNonZero() {};
};

template <typename IDTYPE>
class MRMatrix {
  public:  
    MRMatrix(IDTYPE, IDTYPE, MapReduce *, bool transpose=0, 
             int pagesize=64, const char *fpath=".");
    ~MRMatrix() {delete mr;};

    IDTYPE NumRows() { return N; };
    IDTYPE NumCols() { return M; };
    void MatVec(MRVector<IDTYPE> *, MRVector<IDTYPE> *);
    void Scale(double);
    void MakeEmpty() {delete mr;};
    MapReduce *mr;  // Actual storage; perhaps should be private.
    uint64_t nEmptyRows;  // Number of rows of A with no nonzeros.
    MRVector<IDTYPE> *emptyRows; // Indices of rows of A with no nonzeros (leaf nodes).
  private:
    IDTYPE N;  // Number of rows
    IDTYPE M;  // Number of cols
    bool transposeFlag;  // State variable; indicates whether matrix is stored
                         // as A or as A^T.
};


#endif
