#ifdef MAP_CLASS

MapStyle(print_triangle,MapPrintTriangle)

#else

#ifndef MAP_PRINT_TRIANGLE_H
#define MAP_PRINT_TRIANGLE_H

#include "map.h"

namespace APP_NS {

class MapPrintTriangle : public Map {
 public:
  MapPrintTriangle(class APP *, char *, int, char **);
  ~MapPrintTriangle() {}

 private:
  typedef uint64_t VERTEX;
  
  typedef struct {
    VERTEX vi,vj,vk;
  } TRI;

  static void map(uint64_t, char *, int, char *, int,
		  MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
