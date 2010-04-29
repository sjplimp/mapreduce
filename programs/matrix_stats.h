#ifndef MATRIX_STATS_H
#define MATRIX_STATS_H

#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class MatrixStats {
 public:
  MatrixStats() {}
  double run(MAPREDUCE_NS::MapReduce *, uint64_t);

 private:
  typedef uint64_t VERTEX;
  typedef struct {
    VERTEX vi,vj;
  } EDGE;

  static void map_edge_vert(uint64_t, char *, int, char *, int,
			    MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_sum(char *, int, char *, int, int *, 
			 MAPREDUCE_NS::KeyValue *, void *);
  static void map_invert(uint64_t, char *, int, char *, int,
			 MAPREDUCE_NS::KeyValue *, void *);
  static int compare_uint64(char *, int, char *, int);
  static void map_print(uint64_t, char *, int, char *, int,
			MAPREDUCE_NS::KeyValue *, void *);
};

#endif
