#ifdef REDUCE_CLASS

ReduceStyle(vertex_loser,ReduceVertexLoser)

#else

#ifndef REDUCE_VERTEX_LOSER_H
#define REDUCE_VERTEX_LOSER_H

#include "stdint.h"
#include "reduce.h"

namespace APP_NS {

class ReduceVertexLoser : public Reduce {
 public:
  ReduceVertexLoser(class APP *, char *, int, char **);
  ~ReduceVertexLoser() {}

 private:
  typedef struct {
    uint64_t vi;
    double ri;
    uint64_t vj;
    double rj;
  } EDGE;

  typedef struct {
    uint64_t v;
    double r;
  } VERTEX;

  typedef struct {
    uint64_t v;
    double r;
    int flag;
  } VFLAG;

  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
