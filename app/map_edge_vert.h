#ifdef MAP_CLASS

MapStyle(edge_vert,MapEdgeVert)

#else

#ifndef MAP_EDGE_VERT_H
#define MAP_EDGE_VERT_H

#include "map.h"

namespace APP_NS {

class MapEdgeVert : public Map {
 public:
  MapEdgeVert(class APP *, char *, int, char **);
  ~MapEdgeVert() {}

 private:
  typedef uint64_t VERTEX;

  typedef struct {
    VERTEX vi,vj;
  } EDGE;

  static void map(uint64_t, char *, int, char *, int,
		  MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
