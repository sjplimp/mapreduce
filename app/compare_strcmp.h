#ifdef COMPARE_CLASS

CompareStyle(strcmp,CompareStrCmp)

#else

#ifndef COMPARE_STRCMP_H
#define COMPARE_STRCMP_H

#include "compare.h"

namespace APP_NS {

class CompareStrCmp : public Compare {
 public:
  CompareStrCmp(class APP *, char *, int, char **);
  ~CompareStrCmp() {}

 private:
  static int compare(char *, int, char *, int);
};

}

#endif
#endif
