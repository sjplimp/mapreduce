#ifdef MAP_CLASS

MapStyle(invert_multi,MapInvertMulti)

#else

#ifndef MAP_INVERT_MULTI_H
#define MAP_INVERT_MULTI_H

#include "map.h"

namespace APP_NS {

class MapInvertMulti : public Map {
 public:
  MapInvertMulti(class APP *, char *, int, char **);
  ~MapInvertMulti() {}

 private:
  int nprocs,pshift;

  static void map(uint64_t, char *, int, char *, int,
		  MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
