#ifdef MAP_CLASS

MapStyle(vertex_self,MapVertexSelf)

#else

#ifndef MAP_VERTEX_SELF_H
#define MAP_VERTEX_SELF_H

#include "map.h"

namespace APP_NS {

class MapVertexSelf : public Map {
 public:
  MapVertexSelf(class APP *, char *, int, char **);
  ~MapVertexSelf() {}

 private:
  uint64_t n;
  int me,nprocs;

  static void map(int, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
