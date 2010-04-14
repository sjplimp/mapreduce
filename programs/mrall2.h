// Shared values for MapReduce Vector and Matrix classes.
// Karen Devine, 1416
// June 2008

#ifndef MRALL_H__
#define MRALL_H__

#include <iostream>
#include <list>
#include <stdint.h>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"

using namespace MAPREDUCE_NS;
using namespace std;

#define MIN(a,b) ((a) < (b) ? (a) : (b))

/////////////////////////////////////////////////////////////////////////////
template <typename IDTYPE>
class IDXDOUBLE {
public:
  IDTYPE i;      // When INTDOUBLE is used as a nonzero A_ij in the matrix,
                 // i is the row index of the nonzero.
                 // When INTDOUBLE is used as an entry of vector x, i < 0;
                 // this flag is needed to allow x_j and column j values to
                 // be identified after being reduced.
  double d;      // A_ij or x_j, depending on use above.
  bool operator < (const IDXDOUBLE &v) {return i < v.i;}; // sort by index.
  IDXDOUBLE(){};
  ~IDXDOUBLE(){};
};

#endif
