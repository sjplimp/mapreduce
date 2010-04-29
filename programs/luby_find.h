#ifndef LUBY_FIND_H
#define LUBY_FIND_H

#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class LubyFind {
 public:
  LubyFind(int);
  double run(MAPREDUCE_NS::MapReduce *, MAPREDUCE_NS::MapReduce *, 
	     int &, uint64_t &);

 private:
  int seed;

  typedef struct {
    uint64_t vi,vj;
  } VPAIR;
  typedef struct {
    uint64_t v;
    double r;
  } VERTEX;
  typedef struct {
    uint64_t v;
    double r;
    int flag;
  } VFLAG;
  typedef struct {
    uint64_t vi;
    double ri;
    uint64_t vj;
    double rj;
  } EDGE;

  static void map_vert_random(uint64_t, char *, int, char *, int,
			      MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_edge_winner(char *, int, char *, int, int *, 
				 MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_vert_winner(char *, int, char *, int, int *, 
				 MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_vert_loser(char *, int, char *, int, int *, 
				MAPREDUCE_NS::KeyValue *, void *);
  static void reduce_vert_emit(char *, int, char *, int, int *, 
			       MAPREDUCE_NS::KeyValue *, void *);
};

#endif
