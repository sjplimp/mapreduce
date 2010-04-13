/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#include "mpi.h"
#include "app.h"
#include "input.h"

using namespace APP_NS;

/* ----------------------------------------------------------------------
   main program to drive APP
------------------------------------------------------------------------- */

int main(int argc, char **argv)
{
  MPI_Init(&argc,&argv);

  APP *app = new APP(argc,argv,MPI_COMM_WORLD);
  app->input->file();
  delete app;

  MPI_Finalize();
}
