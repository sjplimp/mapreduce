/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#ifdef COMMAND_CLASS

CommandStyle(rmat,RMAT)

#else

#ifndef APP_RMAT_H
#define APP_RMAT_H

#include "pointers.h"

namespace APP_NS {

class RMAT : protected Pointers {
 public:
  RMAT(class APP *);
  void command(int, char **);
};

}

#endif
#endif
