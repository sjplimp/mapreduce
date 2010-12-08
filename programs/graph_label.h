#ifndef GRAPH_LABEL_H
#define GRAPH_LABEL_H

#include "mpi.h"
#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class GraphLabel {
 public:
  GraphLabel(int, int, int, MPI_Comm);
  void run(MAPREDUCE_NS::MapReduce *);

 private:
  int lo,hi;
  typedef int LABEL;

  static void map(uint64_t, char *, int, char *, int, 
		  MAPREDUCE_NS::KeyValue *, void *);
};

#endif
