#ifdef MAP_CLASS

MapStyle(print_matrix,MapPrintMatrix)

#else

#ifndef MAP_PRINT_MATRIX_H
#define MAP_PRINT_MATRIX_H

#include "map.h"

namespace APP_NS {

class MapPrintMatrix : public Map {
 public:
  MapPrintMatrix(class APP *, char *, int, char **);
  ~MapPrintMatrix() {}

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
