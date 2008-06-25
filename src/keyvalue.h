/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

#ifndef KEY_VALUE_H
#define KEY_VALUE_H

#include "mpi.h"

namespace MAPREDUCE_NS {

class KeyValue {
 public:
  int nkey;                     // # of KV pairs
  int maxkey;                   // max size of keys,values arrays
  int keysize;                  // current size of keydata array
  int maxkeysize;               // max size of keydata
  int valuesize;                // current size of valuedata array
  int maxvaluesize;             // max size of valuedata
  int *keys;                    // keys[i] = offset into keydata for Ith key
  int *values;                  // values[i] = offset into valdata for Ith val
  char *keydata,*valuedata;

  KeyValue(MPI_Comm);
  ~KeyValue();

  void add(char *, int, char *, int);
  void add(int, char *, int, char *, int);
  void add(int, char *, int *, char *, int *);

  int pack(char **);
  void unpack(char *);
  void complete();

 private:
  class Memory *memory;
};

}

#endif
