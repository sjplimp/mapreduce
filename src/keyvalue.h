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
  int keysize;                  // size of keydata array
  int valuesize;                // size of valuedata array
  int *keys;                    // keys[i] = Ith key offset in keydata
  int *values;                  // values[i] = Ith value offset in valuedata
  char *keydata;                // keys, one after another
  char *valuedata;              // values, one after another

  int maxkey;                   // max size of keys,values arrays
  int maxkeysize;               // max size of keydata
  int maxvaluesize;             // max size of valuedata

  int totalsize;                // current size of all 4 arrays in KV
  int chunksize;                // file chunk size for KV

  KeyValue(MPI_Comm);
  ~KeyValue();

  void add(char *, int, char *, int);
  void add(int, char *, int, char *, int);
  void add(int, char *, int *, char *, int *);

  int pack(char **);
  void unpack(char *);
  void complete();

  void add_chunk();
  void write(int);
  void read(int);
  void clean();

 private:
  int me,nprocs;
  class Memory *memory;

  struct Chunk {
    char *filename;
    int nkey;
    int keysize;                  // size of keydata array
    int valuesize;                // size of valuedata array
  };
  Chunk *chunks;
  int nchunk;
};

}

#endif
