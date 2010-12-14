#ifndef SGI_ENUMERATE_H
#define SGI_ENUMERATE_H

#include "mpi.h"
#include "stdio.h"
#include "stdint.h"

namespace MAPREDUCE_NS {
  class MapReduce;
  class KeyValue;
};

class SGIEnumerate {
 public:
  SGIEnumerate(int, int *, int *, int *, MPI_Comm);
  double run(MAPREDUCE_NS::MapReduce *, MAPREDUCE_NS::MapReduce *, 
	     MAPREDUCE_NS::MapReduce *, MAPREDUCE_NS::MapReduce *, 
	     MAPREDUCE_NS::MapReduce *, uint64_t &);

 private:
  int me;
  int itour,ntour;
  int *vtour,*ftour,*etour;
  MPI_Comm world;
  char *outfile;
  FILE *fp;

  typedef uint64_t VERTEX;
  typedef int LABEL;
  typedef struct {
    VERTEX vi,vj;
  } EDGE;
  typedef struct {
    VERTEX vj;
    LABEL fij;
  } X1VALUE;
  typedef struct {
    VERTEX vi;
    LABEL wi,fij;
  } X2VALUE;
  typedef struct {
    VERTEX vj;
    LABEL wi,wj;
    LABEL fij;
  } X3VALUE;

  static void map1(uint64_t, char *, int, char *, int, 
		   MAPREDUCE_NS::KeyValue *, void *);
  static void map2(uint64_t, char *, int, char *, int,
		   MAPREDUCE_NS::KeyValue *, void *);
  static void map3(uint64_t, char *, int, char *, int,
		   MAPREDUCE_NS::KeyValue *, void *);
  static void map4(uint64_t, char *, int, char *, int,
		   MAPREDUCE_NS::KeyValue *, void *);
  static void reduce1a(char *, int, char *, int, int *,
		      MAPREDUCE_NS::KeyValue *, void *);
  static void reduce1b(char *, int, char *, int, int *,
		      MAPREDUCE_NS::KeyValue *, void *);
  static void reduce3(char *, int, char *, int, int *,
		      MAPREDUCE_NS::KeyValue *, void *);

  static void x1print(uint64_t, char *, int, char *, int, 
		      MAPREDUCE_NS::KeyValue *, void *);
  static void x2print(uint64_t, char *, int, char *, int, 
		      MAPREDUCE_NS::KeyValue *, void *);
  static void x3print(uint64_t, char *, int, char *, int, 
		      MAPREDUCE_NS::KeyValue *, void *);
  static void sprint(uint64_t, char *, int, char *, int, 
		     MAPREDUCE_NS::KeyValue *, void *);
  static void rprint(char *, int, char *, int, int *,
		     MAPREDUCE_NS::KeyValue *, void *);
  static void tprint(uint64_t, char *, int, char *, int, 
		     MAPREDUCE_NS::KeyValue *, void *);
};

#endif
