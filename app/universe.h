/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#ifndef APP_UNIVERSE_H
#define APP_UNIVERSE_H

#include "mpi.h"
#include "stdio.h"
#include "pointers.h"

namespace APP_NS {

class Universe : protected Pointers {
 public:
  char *version;          // APP version string = date

  MPI_Comm uworld;        // communicator for entire universe
  int me,nprocs;          // my place in universe

  FILE *uscreen;          // universe screen output
  FILE *ulogfile;         // universe logfile

  int existflag;          // 1 if universe exists due to -partition flag
  int nworlds;            // # of worlds in universe
  int iworld;             // which world I am in
  int *procs_per_world;   // # of procs in each world
  int *root_proc;         // root proc in each world

  Universe(class APP *, MPI_Comm);
  ~Universe();
  void add_world(char *);
  int consistent();
};

}

#endif
