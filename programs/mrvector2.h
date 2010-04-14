// MapReduce Vector class
// Karen Devine, 1416
// April 2010
//
// Stores a vector as a MapReduce object with 
// (Key, Value) = (Global Index i, vector value i)

#include <iostream>
#include <list>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"

#ifndef __MRVECTOR
#define __MRVECTOR

using namespace MAPREDUCE_NS;
using namespace std;

#include "mrall2.h"

/////////////////////////////////////////////////////////////////////////////
template <typename IDTYPE>
class MRVector {
  public:
    MRVector(IDTYPE, int pagesize=64, const char *fpath = ".");
    ~MRVector() {MakeEmpty();}
    void MakeEmpty();
    void PutScalar(double);
    void AddScalar(double);
    void Scale(double);
    double GlobalSum();
    double GlobalMax();
    double GlobalMin();
    double LocalSum();
    double LocalMax();
    double LocalMin();
    IDTYPE GlobalLen() {return global_len;};
    void Print();
    MapReduce *mr;   // Where the data is actually stored; probably should
                     // be private.
  private:
    IDTYPE global_len;
};

#endif
