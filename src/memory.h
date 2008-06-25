/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

#ifndef MEMORY_H
#define MEMORY_H

#include "mpi.h"

namespace MAPREDUCE_NS {

class Memory {
 public:
  Memory(MPI_Comm);
  ~Memory();

  void *smalloc(int n, const char *);
  void sfree(void *);
  void *srealloc(void *, int n, const char *name);

 private:
  class Error *error;
};

}

#endif
