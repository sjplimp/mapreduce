#ifdef HASH_CLASS

HashStyle(simple,HashSimple)

#else

#ifndef HASH_SIMPLE_H
#define HASH_SIMPLE_H

#include "hash.h"

namespace APP_NS {

class HashSimple : public Hash {
 public:
  HashSimple(class APP *, char *, int, char **);
  ~HashSimple() {}

 private:
  static int hash(char *, int);
};

}

#endif
#endif
