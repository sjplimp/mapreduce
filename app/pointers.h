/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

// Pointers class contains ptrs to master copy of
//   fundamental APP class ptrs stored in app.h
// every APP class inherits from Pointers to access app.h ptrs
// these variables are auto-initialized by Pointer class constructor
// *& variables are really pointers to the pointers in app.h
// & enables them to be accessed directly in any class, e.g. error->all()

#ifndef APP_POINTERS_H
#define APP_POINTERS_H

#include "mpi.h"
#include "app.h"

namespace APP_NS {

class Pointers {
 public:
  Pointers(APP *ptr) : 
    app(ptr),
    memory(ptr->memory),
    error(ptr->error),
    universe(ptr->universe),
    input(ptr->input),
    obj(ptr->obj),
    world(ptr->world),
    infile(ptr->infile),
    screen(ptr->screen),
    logfile(ptr->logfile) {}
  virtual ~Pointers() {}

 protected:
  APP *app;
  Memory *&memory;
  Error *&error;
  Universe *&universe;
  Input *&input;

  Object *&obj;

  MPI_Comm &world;
  FILE *&infile;
  FILE *&screen;
  FILE *&logfile;
};

}

#endif
