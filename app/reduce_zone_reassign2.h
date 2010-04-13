#ifdef REDUCE_CLASS

ReduceStyle(zone_reassign2,ReduceZoneReassign2)

#else

#ifndef REDUCE_ZONE_REASSIGN2_H
#define REDUCE_ZONE_REASSIGN2_H

#include "stdint.h"
#include "reduce.h"

namespace APP_NS {

class ReduceZoneReassign2 : public Reduce {
 public:
  ReduceZoneReassign2(class APP *, char *, int, char **);
  ~ReduceZoneReassign2() {}

 private:
  int thresh;
  uint64_t lmask;

  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
