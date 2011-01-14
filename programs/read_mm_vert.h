#ifndef READ_MM_VERT_H
#define READ_MM_VERT_H

#include "mpi.h"
#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class ReadMMVert {
 public:
  ReadMMVert(char *, int, MPI_Comm);
  double run(MAPREDUCE_NS::MapReduce *, uint64_t &);

 private:
  char *file;
  int attflag;
  MPI_Comm world;
  uint64_t nv;
  typedef uint64_t VERTEX;
  typedef int IATTRIBUTE;
  typedef double DATTRIBUTE;

  static void map(int, char *, int, MAPREDUCE_NS::KeyValue *, void *);
};

#endif
