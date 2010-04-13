#ifndef COMPARE_H
#define COMPARE_H

#include "pointers.h"

namespace APP_NS {

class Compare : protected Pointers {
 public:
  char *id;
  int (*appcompare)(char *, int, char *, int);

  Compare(class APP *, char *);
  virtual ~Compare();
};

}

#endif
