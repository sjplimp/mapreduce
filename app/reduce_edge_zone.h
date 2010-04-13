#ifdef REDUCE_CLASS

ReduceStyle(edge_zone,ReduceEdgeZone)

#else

#ifndef REDUCE_EDGE_ZONE_H
#define REDUCE_EDGE_ZONE_H

#include "reduce.h"

namespace APP_NS {

class ReduceEdgeZone : public Reduce {
 public:
  ReduceEdgeZone(class APP *, char *, int, char **);
  ~ReduceEdgeZone() {}

 private:
  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
