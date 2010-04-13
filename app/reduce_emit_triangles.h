#ifdef REDUCE_CLASS

ReduceStyle(emit_triangles,ReduceEmitTriangles)

#else

#ifndef REDUCE_EMIT_TRIANGLES_H
#define REDUCE_EMIT_TRIANGLES_H

#include "stdint.h"
#include "reduce.h"

namespace APP_NS {

class ReduceEmitTriangles : public Reduce {
 public:
  ReduceEmitTriangles(class APP *, char *, int, char **);
  ~ReduceEmitTriangles() {}

 private:
  typedef uint64_t VERTEX;

  typedef struct {
    VERTEX vi,vj;
  } EDGE;
  
  typedef struct {
    VERTEX vi,vj,vk;
  } TRI;

  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
