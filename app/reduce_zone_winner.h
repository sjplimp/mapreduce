#ifdef REDUCE_CLASS

ReduceStyle(zone_winner,ReduceZoneWinner)

#else

#ifndef REDUCE_ZONE_WINNER_H
#define REDUCE_ZONE_WINNER_H

#include "stdint.h"
#include "reduce.h"

namespace APP_NS {

class ReduceZoneWinner : public Reduce {
 public:
  int flag;

  ReduceZoneWinner(class APP *, char *, int, char **);
  ~ReduceZoneWinner() {}

 private:
  typedef struct {
    uint64_t zone,empty;
  } PAD;
  PAD pad;

  static void reduce(char *, int, char *,
		     int, int *, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
