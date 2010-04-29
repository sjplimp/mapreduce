#ifndef MATRIX_UPPER_H
#define MATRIX_UPPER_H

#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class MatrixUpper {
 public:
  MatrixUpper() {}
  double run(MAPREDUCE_NS::MapReduce *, uint64_t &);

 private:
  typedef uint64_t VERTEX;
  typedef struct {
    VERTEX vi,vj;
  } EDGE;

  static void map_invert_drop(uint64_t, char *, int, char *, int,
			      MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_cull(char *, int, char *, int, int *, 
			  MAPREDUCE_NS::KeyValue *, void *);
};

#endif
