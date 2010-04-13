#ifdef REDUCE_CLASS

ReduceStyle(zone_reassign,ReduceZoneReassign)

#else

#ifndef REDUCE_ZONE_REASSIGN_H
#define REDUCE_ZONE_REASSIGN_H

#include "reduce.h"

namespace APP_NS {

class ReduceZoneReassign : public Reduce {
 public:
  ReduceZoneReassign(class APP *, char *, int, char **);
  ~ReduceZoneReassign() {}

 private:
  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
