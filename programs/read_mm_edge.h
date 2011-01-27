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
  ReadMMEdge(int, char **, int, int, MPI_Comm);
  double run(MAPREDUCE_NS::MapReduce *, uint64_t &);

 private:
  int nstr,self,attflag;
  char **strings;
  MPI_Comm world;
  typedef uint64_t VERTEX;
  typedef struct {
    VERTEX vi,vj;
  } EDGE;
  typedef int IATTRIBUTE;
  typedef double DATTRIBUTE;

  static void map(int, char *, MAPREDUCE_NS::KeyValue *, void *);
};

#endif
