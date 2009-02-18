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
    MRVector(MapReduce *, IDTYPE, int store_by_map=0);
    ~MRVector() {MakeEmpty();}
    void AddEntry(VERTEX, double);
    void MakeEmpty();
    void PutScalar(double);
    void AddScalar(double);
    void Scale(double);
    double GlobalSum(MapReduce *);
    double GlobalMax(MapReduce *);
    double GlobalMin(MapReduce *);
    double LocalSum();
    double LocalMax();
    double LocalMin();
    IDTYPE GlobalLen() {return global_len;};
    void EmitEntries(MapReduce *, int);
    void Print();
    list<INTDOUBLE> vec; // Probably should be private; we'll do later.
    bool StorageFormat() {return storeByMap;}
  private:
    IDTYPE global_len;
    int storeByMap;   // Persistent storage either by MapReduce map or not.
};
