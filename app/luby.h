/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#ifdef COMMAND_CLASS

CommandStyle(luby,Luby)

#else

#ifndef APP_LUBY_H
#define APP_LUBY_H

#include "pointers.h"

namespace APP_NS {

class Luby : protected Pointers {
 public:
  Luby(class APP *);
  void command(int, char **);
};

}

#endif
#endif
