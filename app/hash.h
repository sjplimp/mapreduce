#ifndef HASH_H
#define HASH_H

#include "pointers.h"

namespace APP_NS {

class Hash : protected Pointers {
 public:
  char *id;
  int (*apphash)(char *, int);

  Hash(class APP *, char *);
  virtual ~Hash();
};

}

#endif
