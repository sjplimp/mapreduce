/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#ifdef COMMAND_CLASS

CommandStyle(shell,Shell)

#else

#ifndef APP_SHELL_H
#define APP_SHELL_H

#include "pointers.h"

namespace APP_NS {

class Shell : protected Pointers {
 public:
  Shell(class APP *);
  void command(int, char **);

 private:
  int me;
};

}

#endif
#endif
