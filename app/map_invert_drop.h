#ifdef MAP_CLASS

MapStyle(invert_drop,MapInvertDrop)

#else

#ifndef MAP_INVERT_DROP_H
#define MAP_INVERT_DROP_H

#include "map.h"

namespace APP_NS {

class MapInvertDrop : public Map {
 public:
  MapInvertDrop(class APP *, char *, int, char **);
  ~MapInvertDrop() {}

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
