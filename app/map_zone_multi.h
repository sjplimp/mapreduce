#ifdef MAP_CLASS

MapStyle(zone_multi,MapZoneMulti)

#else

#ifndef MAP_ZONE_MULTI_H
#define MAP_ZONE_MULTI_H

#include "map.h"

namespace APP_NS {

class MapZoneMulti : public Map {
 public:
  MapZoneMulti(class APP *, char *, int, char **);
  ~MapZoneMulti() {}

 private:
  int nprocs,pshift;


  uint64_t lmask;


  static void map(uint64_t, char *, int, char *, int,
		  MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
