/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

#ifndef MAP_REDUCE_H
#define MAP_REDUCE_H

#include "mpi.h"

namespace MAPREDUCE_NS {

class MapReduce {
 public:
  int mapstyle;     // 0 = chunks, 1 = strided, 2 = master/slave
  int verbosity;    // 0 = none, 1 = totals, 2 = proc histograms

  static MapReduce *mrptr;

  MapReduce(MPI_Comm);
  ~MapReduce();

  int aggregate(int (*)(char *, int));
  int clone();
  int collapse(char *, int);
  int collate(int (*)(char *, int));
  int compress(void (*)(char *, int, char **, class KeyValue *, void *),
	       void *);
  int convert();
  int gather(int);
  int map(int, void (*)(int, class KeyValue *, void *), void *);
  int reduce(void (*)(char *, int, char **, class KeyValue *, void *), void *);
  int scrunch(int, char *, int);
  int sort_keys(int (*)(char *, char *));
  int sort_values(int (*)(char *, char *));
  int sort_multivalues(int (*)(char *, char *));

  void kv_stats(int);
  void kmv_stats(int);

  void sort_kv(int);
  int compare_keys(int, int);
  int compare_values(int, int);

  void stats(char *, int, int);
  void callback(class KeyValue *, class KeyMultiValue *, class KeyValue *,
		void (*)(char *, int, char **, class KeyValue *, void *),
		void *);
  void histogram(int, double *, double &, double &, double &,
		 int, int *, int *);

 private:
  MPI_Comm comm;
  int me,nprocs;
  class Memory *memory;
  class Error *error;

  class KeyValue *kv;
  class KeyMultiValue *kmv;

  typedef int (CompareFunc)(char *, char *);
  CompareFunc *compare;
};

}

#endif
