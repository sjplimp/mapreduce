#ifdef REDUCE_CLASS

ReduceStyle(edge_winner,ReduceEdgeWinner)

#else

#ifndef REDUCE_EDGE_WINNER_H
#define REDUCE_EDGE_WINNER_H

#include "stdint.h"
#include "reduce.h"

namespace APP_NS {

class ReduceEdgeWinner : public Reduce {
 public:
  ReduceEdgeWinner(class APP *, char *, int, char **);
  ~ReduceEdgeWinner() {}

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
