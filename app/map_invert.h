#ifdef MAP_CLASS

MapStyle(invert,MapInvert)

#else

#ifndef MAP_INVERT_H
#define MAP_INVERT_H

#include "map.h"

namespace APP_NS {

class MapInvert : public Map {
 public:
  MapInvert(class APP *, char *, int, char **);
  ~MapInvert() {}

 private:
  static void map(uint64_t, char *, int, char *, int,
		  MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
