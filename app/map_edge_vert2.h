#ifdef MAP_CLASS

MapStyle(edge_vert2,MapEdgeVert2)

#else

#ifndef MAP_EDGE_VERT2_H
#define MAP_EDGE_VERT2_H

#include "map.h"

namespace APP_NS {

class MapEdgeVert2 : public Map {
 public:
  MapEdgeVert2(class APP *, char *, int, char **);
  ~MapEdgeVert2() {}

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
