/* ----------------------------------------------------------------------
   OINK - Mapreduce-MPI library application
   http://www.sandia.gov/~sjplimp/mapreduce.html, Sandia National Laboratories
   Steve Plimpton, sjplimp@sandia.gov

   See the README file in the top-level MR-MPI directory.
------------------------------------------------------------------------- */

#ifdef COMMAND_CLASS

CommandStyle(rmat,RMAT)

#else

#ifndef OINK_RMAT_H
#define OINK_RMAT_H

#include "command.h"
#include "map_rmat_generate.h"
#include "mapreduce.h"
using namespace MAPREDUCE_NS;

namespace OINK_NS {

class RMAT : public Command {
 public:
  RMAT(class OINK *);
  void run();
  void params(int, char **);

 private:
  RMAT_struct rmat;
  int statflag;
};

}

#endif
#endif
