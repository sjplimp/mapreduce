/* ----------------------------------------------------------------------
   OINK - scripting wrapper on MapReduce-MPI library
   http://www.sandia.gov/~sjplimp/mapreduce.html, Sandia National Laboratories
   Steve Plimpton, sjplimp@sandia.gov

   See the README file in the top-level MR-MPI directory.
------------------------------------------------------------------------- */

#ifdef COMMAND_CLASS

CommandStyle(degree,Degree)

#else

#ifndef OINK_DEGREE_H
#define OINK_DEGREE_H

#include "command.h"
#include "mapreduce.h"
using namespace MAPREDUCE_NS;

namespace OINK_NS {

class Degree : public Command {
 public:
  Degree(class OINK *);
  void run();
  void params(int, char **);

 private:
  static void read(int, char *, KeyValue *, void *);
  static void print(char *, int, char *, int, void *);
  static void map1(uint64_t, char *, int, char *, int, KeyValue *, void *);
  static void reduce1(char *, int, char *, int, int *, KeyValue *, void *);
};

}

#endif
#endif
