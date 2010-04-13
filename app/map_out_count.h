#ifdef MAP_CLASS

MapStyle(out_count,MapOutCount)

#else

#ifndef MAP_OUT_COUNT_H
#define MAP_OUT_COUNT_H

#include "map.h"

namespace APP_NS {

class MapOutCount : public Map {
 public:
  MapOutCount(class APP *, char *, int, char **);
  ~MapOutCount() {}

 private:
  int n,limit,flag;

  static void map(uint64_t, char *, int, char *, int,
		  MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
