#ifndef TRI_FIND_H
#define TRI_FIND_H

#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class TriFind {
 public:
  TriFind() {}
  double run(MAPREDUCE_NS::MapReduce *, MAPREDUCE_NS::MapReduce *, uint64_t &);

 private:
  typedef uint64_t VERTEX;
  typedef struct {
    VERTEX vi,vj;
  } EDGE;
  typedef struct {
    int di,dj;
  } DEGREE;
  typedef struct {
    VERTEX vi,vj,vk;
  } TRI;

  static void map_edge_vert(uint64_t, char *, int, char *, int,
			    MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_first_degree(char *, int, char *, int, int *, 
				  MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_second_degree(char *, int, char *, int, int *, 
				   MAPREDUCE_NS::KeyValue *, void *);
  static void map_low_degree(uint64_t, char *, int, char *, int,
			     MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_nsq_angles(char *, int, char *, int, int *, 
				MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_emit_triangles(char *, int, char *, int, int *, 
				    MAPREDUCE_NS::KeyValue *, void *);
};

#endif
