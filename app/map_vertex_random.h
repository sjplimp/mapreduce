#ifdef MAP_CLASS

MapStyle(vertex_random,MapVertexRandom)

#else

#ifndef MAP_VERTEX_RANDOM_H
#define MAP_VERTEX_RANDOM_H

#include "map.h"

namespace APP_NS {

class MapVertexRandom : public Map {
 public:
  MapVertexRandom(class APP *, char *, int, char **);
  ~MapVertexRandom() {}

 private:
  typedef struct {
    uint64_t vi,vj;
  } VPAIR;

  typedef struct {
    uint64_t vi;
    double ri;
    uint64_t vj;
    double rj;
  } EDGE;

  static void map(uint64_t, char *, int, char *, int,
		  MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
