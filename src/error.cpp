/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

#include "mpi.h"
#include "stdlib.h"
#include "stdio.h"
#include "error.h"

using namespace MAPREDUCE_NS;

/* ---------------------------------------------------------------------- */

Error::Error(MPI_Comm caller)
{
  comm = caller;
  MPI_Comm_rank(comm,&me);
}

/* ----------------------------------------------------------------------
   called by all procs
------------------------------------------------------------------------- */

void Error::all(const char *str)
{
  if (me == 0) printf("ERROR: %s\n",str);
  MPI_Finalize();
  exit(1);
}

/* ----------------------------------------------------------------------
   called by one proc
------------------------------------------------------------------------- */

void Error::one(const char *str)
{
  printf("ERROR on proc %d: %s\n",me,str);
  MPI_Abort(comm,1);
}

/* ----------------------------------------------------------------------
   called by one proc
------------------------------------------------------------------------- */

void Error::warning(const char *str)
{
  printf("WARNING: %s\n",str);
}
