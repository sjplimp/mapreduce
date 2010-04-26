#ifdef REDUCE_CLASS

ReduceStyle(vertex_winner,ReduceVertexWinner)

#else

#ifndef REDUCE_VERTEX_WINNER_H
#define REDUCE_VERTEX_WINNER_H

#include "stdint.h"
#include "reduce.h"

namespace APP_NS {

class ReduceVertexWinner : public Reduce {
 public:
  ReduceVertexWinner(class APP *, char *, int, char **);
  ~ReduceVertexWinner() {}

 private:
  MAPREDUCE_NS::KeyValue *kv;

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
