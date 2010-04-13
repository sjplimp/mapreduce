#ifndef REDUCE_H
#define REDUCE_H

#include "pointers.h"

namespace MAPREDUCE_NS {
  class KeyValue;
};

namespace APP_NS {

class Reduce : protected Pointers {
 public:
  char *id;
  void (*appreduce)(char *, int, char *,
		    int, int *, MAPREDUCE_NS::KeyValue *, void *);
  void *appptr;

  Reduce(class APP *, char *);
  virtual ~Reduce();
};

}

#endif
