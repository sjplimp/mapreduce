#ifdef MAP_CLASS

MapStyle(rmat_edge,MapRmatEdge)

#else

#ifndef MAP_RMAT_EDGE_H
#define MAP_RMAT_EDGE_H

#include "map.h"

namespace APP_NS {

class MapRmatEdge : public Map {
 public:
  uint64_t ngenerate;

  MapRmatEdge(class APP *, char *, int, char **);
  ~MapRmatEdge() {}

 private:
  uint64_t nrows;
  int nlevels;
  double a,b,c,d,fraction;
  int me,nprocs;

  typedef struct {
    uint64_t vi,vj;
  } EDGE;

  static void map(int, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
