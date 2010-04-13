#ifdef REDUCE_CLASS

ReduceStyle(rmat_cull,ReduceRmatCull)

#else

#ifndef REDUCE_RMAT_CULL_H
#define REDUCE_RMAT_CULL_H

#include "reduce.h"

namespace APP_NS {

class ReduceRmatCull : public Reduce {
 public:
  ReduceRmatCull(class APP *, char *, int, char **);
  ~ReduceRmatCull() {}

 private:
  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
