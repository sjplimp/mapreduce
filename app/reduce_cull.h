#ifdef REDUCE_CLASS

ReduceStyle(cull,ReduceCull)

#else

#ifndef REDUCE_CULL_H
#define REDUCE_CULL_H

#include "reduce.h"

namespace APP_NS {

class ReduceCull : public Reduce {
 public:
  ReduceCull(class APP *, char *, int, char **);
  ~ReduceCull() {}

 private:
  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
