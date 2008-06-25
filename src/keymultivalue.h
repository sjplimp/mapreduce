/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

#ifndef KEY_MULTIVALUE_H
#define KEY_MULTIVALUE_H

#include "mpi.h"

namespace MAPREDUCE_NS {

class KeyMultiValue {
 public:
  struct KeyEntry {     // a unique key
    int keyindex;       // index in KV for this key
    int count;          // # of values for this key
    int valuehead;      // index in KV of 1st value for this key
    int next;           // index in keys of next key in this hash bucket
  };

  int nkey;             // # of unique keys in KMV
  int maxkey;           // max # of unique keys in keys array
  KeyEntry *keys;       // one entry per unique key
  int *valueindex;      // vindex[i] = next value with same key, -1 for last
                        // length of valueindex = # of values in KV

  int cloned;           // 1 if this KMV was cloned from a KV
  int collapsed;        // 1 if this KMV was collapsed from a KV
  char *singlekey;      // the single key for a collapsed KMV
  int singlekeylen;     // key length of single key

  int maxdepth;         // max depth of any one hash bucket

  KeyMultiValue(MPI_Comm);
  ~KeyMultiValue();

  void create(class KeyValue *);
  void grow_buckets(KeyValue *);
  int hash(char *, int);
  int find(int, char *, int, class KeyValue *);
  int match(char *, char *, int);

 private:
  class Memory *memory;

  int *buckets;         // bucket[i] = index of 1st key entry in this bucket
  int nbuckets;         // # of hash buckets
  int hashmask;         // bit mask for mapping into hash buckets
};

}

#endif
