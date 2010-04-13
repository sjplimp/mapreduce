#ifdef MAP_CLASS

MapStyle(edge_vertex,MapEdgeVertex)

#else

#ifndef MAP_EDGE_VERTEX_H
#define MAP_EDGE_VERTEX_H

#include "map.h"

namespace APP_NS {

class MapEdgeVertex : public Map {
 public:
  MapEdgeVertex(class APP *, char *, int, char **);
  ~MapEdgeVertex() {}

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
