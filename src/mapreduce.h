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
#include "stdint.h"

namespace MAPREDUCE_NS {

class MapReduce {
 public:
  int mapstyle;     // 0 = chunks, 1 = strided, 2 = master/slave
  int verbosity;    // 0 = none, 1 = totals, 2 = proc histograms

  static MapReduce *mrptr;

  // library API

  MapReduce(MPI_Comm);
  ~MapReduce();

  int aggregate(int (*)(char *, int));
  int clone();
  int collapse(char *, int);
  int collate(int (*)(char *, int));
  int compress(void (*)(char *, int, char *,
			int, int *, class KeyValue *, void *),
	       void *);
  int convert();
  int gather(int);
  int map(int, void (*)(int, class KeyValue *, void *),
	  void *, int addflag = 0);
  int map(int, int, char **, char, int, 
	  void (*)(int, char *, int, class KeyValue *, void *),
	  void *, int addflag = 0);
  int map(int, int, char **, char *, int, 
	  void (*)(int, char *, int, class KeyValue *, void *),
	  void *, int addflag = 0);
  int reduce(void (*)(char *, int, char *,
		      int, int *, class KeyValue *, void *),
	     void *);
  int scrunch(int, char *, int);
  int sort_keys(int (*)(char *, int, char *, int));
  int sort_values(int (*)(char *, int, char *, int));
  int sort_multivalues(int (*)(char *, int, char *, int));

  void kv_stats(int);
  void kmv_stats(int);

  // functions accessed thru non-class wrappers

  void map_file_user(int, class KeyValue *, void *);
  int compare_keys(int, int);
  int compare_values(int, int);
  int compare_multivalues(int, int);

 private:
  MPI_Comm comm;
  int me,nprocs;
  class Memory *memory;
  class Error *error;

  class KeyValue *kv;
  class KeyMultiValue *kmv;

  typedef int (CompareFunc)(char *, int, char *, int);
  CompareFunc *compare;

  char **mv_values;      // used by sort_multivalues()
  int *mv_valuesizes;

  struct FileMap {       // used by file map()
    int sepwhich;
    char sepchar;
    char *sepstr;
    int delta;
    char **filename;
    uint64_t *filesize;
    int *tasksperfile;
    int *whichfile;
    int *whichtask;
    typedef void (MapFileFunc)(int, char *, int, class KeyValue *, void *);
    MapFileFunc *appmapfile;
  };
  FileMap filemap;

  int mapfile(int, int, char **,
	      void (*)(int, char *, int, class KeyValue *, void *),
	      void *, int addflag);
  void sort_kv(int);
  void stats(char *, int, int);
  void histogram(int, double *, double &, double &, double &,
		 int, int *, int *);
};

}

#endif
