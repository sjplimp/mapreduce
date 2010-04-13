#ifdef REDUCE_CLASS

ReduceStyle(first_degree,ReduceFirstDegree)

#else

#ifndef REDUCE_FIRST_DEGREE_H
#define REDUCE_FIRST_DEGREE_H

#include "stdint.h"
#include "reduce.h"

namespace APP_NS {

class ReduceFirstDegree : public Reduce {
 public:
  ReduceFirstDegree(class APP *, char *, int, char **);
  ~ReduceFirstDegree() {}

 private:
  typedef uint64_t VERTEX;

  typedef struct {
    VERTEX vi,vj;
  } EDGE;

  typedef struct {
    int di,dj;
  } DEGREE;
  
  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
