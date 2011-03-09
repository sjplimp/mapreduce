/* ----------------------------------------------------------------------
   OINK - scripting wrapper on MapReduce-MPI library
   http://www.sandia.gov/~sjplimp/mapreduce.html, Sandia National Laboratories
   Steve Plimpton, sjplimp@sandia.gov

   See the README file in the top-level MR-MPI directory.
------------------------------------------------------------------------- */

// typedefs used by various classes

#ifndef OINK_TYPEDEFS_H
#define OINK_TYPEDEFS_H

#define __STDC_LIMIT_MACROS
#include "stdint.h"
#include "keyvalue.h"
using MAPREDUCE_NS::KeyValue;

typedef uint64_t VERTEX;
typedef struct {
  VERTEX vi,vj;
} EDGE;

typedef int (*HashFnPtr)(char *, int);
typedef int (*CompareFnPtr)(char *, int, char *, int);
typedef void (*MapTaskFnPtr)(int, KeyValue *, void *);
typedef void (*MapFileFnPtr)(int, char *, KeyValue *, void *);
typedef void (*MapStringFnPtr)(int, char *, int, KeyValue *, void *);
typedef void (*MapMRFnPtr)(uint64_t, char *, int, char *, 
			   int, KeyValue *, void *);
typedef void (*ReduceFnPtr)(char *, int, char *, int, int *, 
			    KeyValue *, void *);
typedef void (*ScanKVFnPtr)(char *, int, char *, int, void *);
typedef void (*ScanKMVFnPtr)(char *, int, char *, int, int *, void *);

#endif
