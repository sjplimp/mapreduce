/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#ifdef COMMAND_CLASS

CommandStyle(cc,CC)

#else

#ifndef APP_CC_H
#define APP_CC_H

#include "pointers.h"

namespace APP_NS {

class CC : protected Pointers {
 public:
  CC(class APP *);
  void command(int, char **);
};

}

#endif
#endif
