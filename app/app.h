/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#ifndef APP_APP_H
#define APP_APP_H

#include "mpi.h"
#include "stdio.h"

namespace APP_NS {

class APP {
 public:
                                 // ptrs to fundamental APP classes
  class Memory *memory;          // memory allocation functions
  class Error *error;            // error handling
  class Universe *universe;      // universe of processors
  class Input *input;            // input script processing
                                 // ptrs to top-level APP-specific classes
  class Object *obj;             // object classes and instances

                                 // MPI and I/O for my world of procs
  MPI_Comm world;                // MPI communicator
  FILE *infile;                  // infile
  FILE *screen;                  // screen output
  FILE *logfile;                 // logfile

  APP(int, char **, MPI_Comm);
  ~APP();
  void create();
  void init();
  void destroy();
};

}

#endif
