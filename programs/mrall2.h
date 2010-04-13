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
//  These should probably be in the include file for MapReduce library.
typedef void MAPFUNCTION(int, KeyValue *, void *);
typedef void MAPFILEFUNCTION(int, char *, int, KeyValue *, void *);
typedef void REDUCEFUNCTION(char *, int, char *, int, int*, KeyValue *, void *);
typedef int COMPAREFUNCTION(char *, int, char *, int);


/////////////////////////////////////////////////////////////////////////////
//  Data type for matrix and vector row/column indices.  
//  For small problems, use uint32_t; for more than 2^31 rows, use uint64_t.  
//  Change MPI_IDTYPE appropriately:  MPI_INT or MPI_LONG.
//  Change routine for conversion from string appropriately:  atoi or atol.
//  Eventually, do templating here.

#define KDD32BIT
#ifdef KDD32BIT
typedef uint32_t IDTYPE;
#define MPI_IDTYPE MPI_INT 
#define ATOID atoi
#define IDFORMAT "%u"
#else
typedef uint64_t IDTYPE;
#define MPI_IDTYPE MPI_LONG
#define ATOID atol
#define IDFORMAT "%lu"
#endif

/////////////////////////////////////////////////////////////////////////////
//  Data type for values emitted.
#define XVECVALUE -1

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

static bool compare_idxdouble(IDXDOUBLE x, IDXDOUBLE y)
{
  if (x.i < y.i) return true;
  else return false;
}


#endif
