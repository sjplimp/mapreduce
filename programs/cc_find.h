#ifndef CC_FIND_H
#define CC_FIND_H

#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class CCFind {
 public:
  CCFind(uint64_t, int);
  double run(MAPREDUCE_NS::MapReduce *, MAPREDUCE_NS::MapReduce *, 
	     MAPREDUCE_NS::MapReduce *, int &);

 private:
  int me,nprocs;
  uint64_t nvert;
  int nthresh;
  int flag,pshift;
  uint64_t lmask;

  typedef uint64_t VERTEX;
  typedef struct {
    VERTEX vi,vj;
  } EDGE;
  typedef struct {
    uint64_t zone,empty;
  } PAD;
  PAD pad;

  static void map_vert_self(int, MAPREDUCE_NS::KeyValue *, void *);
  static void map_edge_vert(uint64_t, char *, int, char *, int,
			    MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_edge_zone(char *, int, char *, int, int *, 
			       MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_zone_winner(char *, int, char *, int, int *, 
				 MAPREDUCE_NS::KeyValue *, void *);
  static void map_invert_multi(uint64_t, char *, int, char *, int,
			       MAPREDUCE_NS::KeyValue *, void *);
  static void map_zone_multi(uint64_t, char *, int, char *, int,
			     MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_zone_reassign(char *, int, char *, int, int *, 
				   MAPREDUCE_NS::KeyValue *, void *);
  static void map_strip(uint64_t, char *, int, char *, int,
			MAPREDUCE_NS::KeyValue *, void *);
};

#endif
