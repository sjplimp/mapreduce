/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

#ifndef ERROR_H
#define ERROR_H

#include "mpi.h"

namespace MAPREDUCE_NS {

class Error {
 public:
  Error(MPI_Comm);

  void all(const char *);
  void one(const char *);
  void warning(const char *);

 private:
  MPI_Comm comm;
  int me;
};

}

#endif
