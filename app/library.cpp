/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

// C or Fortran style library interface to APP
// new APP-specific functions can be added

#include "mpi.h"
#include "library.h"
#include "app.h"
#include "input.h"

using namespace APP_NS;

/* ----------------------------------------------------------------------
   create an instance of APP and return pointer to it
   pass in command-line args and MPI communicator to run on
------------------------------------------------------------------------- */

void app_open(int argc, char **argv, MPI_Comm communicator, void **ptr)
{
  APP *app = new APP(argc,argv,communicator);
  *ptr = (void *) app;
}

/* ----------------------------------------------------------------------
   destruct an instance of APP
------------------------------------------------------------------------- */

void app_close(void *ptr)
{
  APP *app = (APP *) ptr;
  delete app;
}

/* ----------------------------------------------------------------------
   process an input script in filename str
------------------------------------------------------------------------- */

void app_file(void *ptr, char *str)
{
  APP *app = (APP *) ptr;
  app->input->file(str);
}

/* ----------------------------------------------------------------------
   process a single input command in str
------------------------------------------------------------------------- */

char *app_command(void *ptr, char *str)
{
  APP *app = (APP *) ptr;
  return app->input->one(str);
}

/* ----------------------------------------------------------------------
   add APP-specific library functions
   all must receive APP pointer as argument
------------------------------------------------------------------------- */
