#ifdef REDUCE_CLASS

ReduceStyle(nsq_angles,ReduceNsqAngles)

#else

#ifndef REDUCE_NSQ_ANGLES_H
#define REDUCE_NSQ_ANGLES_H

#include "stdint.h"
#include "reduce.h"

namespace APP_NS {

class ReduceNsqAngles : public Reduce {
 public:
  ReduceNsqAngles(class APP *, char *, int, char **);
  ~ReduceNsqAngles() {}

 private:
  typedef uint64_t VERTEX;

  typedef struct {
    VERTEX vi,vj;
  } EDGE;
  
  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
