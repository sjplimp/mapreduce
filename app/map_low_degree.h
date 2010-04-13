#ifdef MAP_CLASS

MapStyle(low_degree,MapLowDegree)

#else

#ifndef MAP_LOW_DEGREE_H
#define MAP_LOW_DEGREE_H

#include "map.h"

namespace APP_NS {

class MapLowDegree : public Map {
 public:
  MapLowDegree(class APP *, char *, int, char **);
  ~MapLowDegree() {}

 private:
  typedef uint64_t VERTEX;

  typedef struct {
    VERTEX vi,vj;
  } EDGE;

  typedef struct {
    int di,dj;
  } DEGREE;

  static void map(uint64_t, char *, int, char *, int,
		  MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
