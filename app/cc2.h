/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#ifdef COMMAND_CLASS

CommandStyle(cc2,CC2)

#else

#ifndef APP_CC2_H
#define APP_CC2_H

#include "pointers.h"

namespace APP_NS {

class CC2 : protected Pointers {
 public:
  CC2(class APP *);
  void command(int, char **);
};

}

#endif
#endif
