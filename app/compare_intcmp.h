#ifdef COMPARE_CLASS

CompareStyle(intcmp,CompareIntCmp)

#else

#ifndef COMPARE_INTCMP_H
#define COMPARE_INTCMP_H

#include "compare.h"

namespace APP_NS {

class CompareIntCmp : public Compare {
 public:
  CompareIntCmp(class APP *, char *, int, char **);
  ~CompareIntCmp() {}

 private:
  static int compare(char *, int, char *, int);
};

}

#endif
#endif
