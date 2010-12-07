#ifndef MATRIX_UPPER_H
#define MATRIX_UPPER_H

#include "mpi.h"
#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class MatrixUpper {
 public:
  MatrixUpper(MPI_Comm);
  double run(MAPREDUCE_NS::MapReduce *, uint64_t &);

 private:
  MPI_Comm world;
  typedef uint64_t VERTEX;
  typedef struct {
    VERTEX vi,vj;
  } EDGE;

  static void map(uint64_t, char *, int, char *, int,
		  MAPREDUCE_NS::KeyValue *, void *);
  static void reduce(char *, int, char *, int, int *, 
		     MAPREDUCE_NS::KeyValue *, void *);
};

#endif
