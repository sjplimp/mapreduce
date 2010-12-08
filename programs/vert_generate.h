#ifndef VERT_GENERATE_H
#define VERT_GENERATE_H

#include "mpi.h"
#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class VertGenerate {
 public:
  VertGenerate(uint64_t, int, MPI_Comm);
  void run(MAPREDUCE_NS::MapReduce *);

 private:
  uint64_t nvert;
  int start;
  int me,nprocs;
  typedef uint64_t VERTEX;

  static void map(int, MAPREDUCE_NS::KeyValue *, void *);
};

#endif
