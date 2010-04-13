#ifdef MAP_CLASS

MapStyle(file_read,MapFileRead)

#else

#ifndef MAP_FILE_READ_H
#define MAP_FILE_READ_H

#include "map.h"

namespace APP_NS {

class MapFileRead : public Map {
 public:
  MapFileRead(class APP *, char *, int, char **);
  ~MapFileRead() {}

 private:
  static void map(int, char *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
