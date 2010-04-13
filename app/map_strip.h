#ifdef MAP_CLASS

MapStyle(strip,MapStrip)

#else

#ifndef MAP_STRIP_H
#define MAP_STRIP_H

#include "map.h"

namespace APP_NS {

class MapStrip : public Map {
 public:
  MapStrip(class APP *, char *, int, char **);
  ~MapStrip() {}

 private:
  static void map(uint64_t, char *, int, char *, int,
		  MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
