#ifndef RMAT_GENERATE_H
#define RMAT_GENERATE_H

#include "mpi.h"
#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class RMATGenerate {
 public:
  RMATGenerate(uint64_t, uint64_t,
	       double, double, double, double, double, int, MPI_Comm);
  double run(MAPREDUCE_NS::MapReduce *, int &);

 private:
  uint64_t nvert,nedge;
  double a,b,c,d,fraction;
  int seed;

  MPI_Comm world;
  int me,nprocs;
  int nlevels;
  uint64_t ngenerate;

  typedef uint64_t VERTEX;
  typedef struct {
    VERTEX vi,vj;
  } EDGE;

  static void map_rmat_edge(int, MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_cull(char *, int, char *,
			  int, int *, MAPREDUCE_NS::KeyValue *, void *);

};

#endif
