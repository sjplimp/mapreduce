#ifdef REDUCE_CLASS

ReduceStyle(second_degree,ReduceSecondDegree)

#else

#ifndef REDUCE_SECOND_DEGREE_H
#define REDUCE_SECOND_DEGREE_H

#include "stdint.h"
#include "reduce.h"

namespace APP_NS {

class ReduceSecondDegree : public Reduce {
 public:
  ReduceSecondDegree(class APP *, char *, int, char **);
  ~ReduceSecondDegree() {}

 private:
  typedef struct {
    int di,dj;
  } DEGREE;
  
  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
