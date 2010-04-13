/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#ifndef APP_ERROR_H
#define APP_ERROR_H

#include "pointers.h"

namespace APP_NS {

class Error : protected Pointers {
 public:
  Error(class APP *);

  void universe_all(const char *);
  void universe_one(const char *);

  void all(const char *);
  void one(const char *);
  void warning(const char *);
};

}

#endif
