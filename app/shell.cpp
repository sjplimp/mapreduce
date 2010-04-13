/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#include "mpi.h"
#include "string.h"
#include "unistd.h"
#include "sys/stat.h"
#include "shell.h"
#include "error.h"

using namespace APP_NS;

/* ---------------------------------------------------------------------- */

Shell::Shell(APP *app) : Pointers(app)
{
  MPI_Comm_rank(world,&me);
}

/* ---------------------------------------------------------------------- */

void Shell::command(int narg, char **arg)
{
  if (narg < 1) error->all("Illegal shell command");

  if (strcmp(arg[0],"cd") == 0) {
    if (narg != 2) error->all("Illegal shell command");
    chdir(arg[1]);

  } else if (strcmp(arg[0],"mkdir") == 0) {
    if (narg < 2) error->all("Illegal shell command");
#ifndef WINDOWS
    if (me == 0)
      for (int i = 1; i < narg; i++)
	mkdir(arg[i], S_IRWXU | S_IRGRP | S_IXGRP);
#endif

  } else if (strcmp(arg[0],"mv") == 0) {
    if (narg != 3) error->all("Illegal shell command");
    if (me == 0) rename(arg[1],arg[2]);

  } else if (strcmp(arg[0],"rm") == 0) {
    if (narg < 2) error->all("Illegal shell command");
    if (me == 0)
      for (int i = 1; i < narg; i++)
	unlink(arg[i]);

  } else if (strcmp(arg[0],"rmdir") == 0) {
    if (narg < 2) error->all("Illegal shell command");
    if (me == 0)
      for (int i = 1; i < narg; i++)
	rmdir(arg[i]);

  } else error->all("Illegal shell command");
}
