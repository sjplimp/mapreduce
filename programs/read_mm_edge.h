#ifndef READ_MM_EDGE_H
#define READ_MM_EDGE_H

#include "mpi.h"
#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class ReadMMEdge {
 public:
  ReadMMEdge(char *, int, MPI_Comm);
  double run(MAPREDUCE_NS::MapReduce *, uint64_t &, uint64_t &);

 private:
  char *file;
  int wtflag;
  MPI_Comm world;
  int nv;
  typedef uint64_t VERTEX;
  typedef struct {
    VERTEX vi,vj;
  } EDGE;
  typedef int IWEIGHT;
  typedef double DWEIGHT;

  static void map(int, char *, int, MAPREDUCE_NS::KeyValue *, void *);
};

#endif
