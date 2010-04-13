#ifdef REDUCE_CLASS

ReduceStyle(sum,ReduceSum)

#else

#ifndef REDUCE_SUM_H
#define REDUCE_SUM_H

#include "reduce.h"

namespace APP_NS {

class ReduceSum : public Reduce {
 public:
  ReduceSum(class APP *, char *, int, char **);
  ~ReduceSum() {}

 private:
  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
