/* ----------------------------------------------------------------------
   MR-MPI = MapReduce-MPI library
   http://www.cs.sandia.gov/~sjplimp/mapreduce.html
   Steve Plimpton, sjplimp@sandia.gov, Sandia National Laboratories

   Copyright (2009) Sandia Corporation.  Under the terms of Contract
   DE-AC04-94AL85000 with Sandia Corporation, the U.S. Government retains
   certain rights in this software.  This software is distributed under 
   the modified Berkeley Software Distribution (BSD) License.

   See the README file in the top-level MapReduce directory.
------------------------------------------------------------------------- */

#ifndef KEY_MULTIVALUE_H
#define KEY_MULTIVALUE_H

#include "mpi.h"
#include "stdio.h"
#include "stdint.h"

namespace MAPREDUCE_NS {

class KeyMultiValue {
 public:
  uint64_t nkmv;                   // # of KMV pairs in entire KMV on this proc
  uint64_t ksize;                  // exact size of all key data
  uint64_t vsize;                  // exact size of all multivalue data
  uint64_t tsize;                  // total exact size of entire KMV
  int fileflag;                    // 1 if file exists, 0 if not

  static uint64_t rsize,wsize;     // total file read/write for all KMVs
  
  KeyMultiValue(MPI_Comm, char *, uint64_t, int, int, char *);
  ~KeyMultiValue();

  void reset_page(char *);
  void copy(KeyMultiValue *);
  void complete();
  int request_info(char **);
  int request_page(int, int, uint64_t &, uint64_t &, uint64_t &);
  uint64_t multivalue_blocks(int, int &);
  void overwrite_page(int);
  void close_file();

  void clone(class KeyValue *);
  void collapse(char *, int, class KeyValue *);
  int convert(class KeyValue *, char *, uint64_t, char *);

 private:
  int me;
  MPI_Comm comm;
  class Memory *memory;
  class Error *error;

  int kalign,valign;                 // alignment for keys & multivalues
  int talign;                        // alignment of entire KMV pair
  int ualign;                        // alignment of Unique
  int kalignm1,valignm1;             // alignments-1 for masking
  int talignm1,ualignm1;
  int twolenbytes;                   // size of key & value lengths
  int threelenbytes;                 // size of nvalue & key & value lengths

  // in-memory page

  int nkey;                          // # of KMV pairs in page
  uint64_t nvalue;                   // # of values in all KMV mvalues in page
  uint64_t keysize;                  // exact size of key data in page
  uint64_t valuesize;                // exact size of multivalue data in page
  uint64_t alignsize;                // current size of page with alignment

  char *page;                   // in-memory page
  uint64_t pagesize;            // size of page

  // virtual pages

  struct Page {
    uint64_t keysize;           // exact size of keys 
    uint64_t valuesize;         // exact size of multivalues
    uint64_t exactsize;         // exact size of all data in page
    uint64_t alignsize;         // aligned size of all data in page
    uint64_t filesize;          // rounded-up alignsize for file I/O
    uint64_t fileoffset;        // summed filesize of all previous pages
    uint64_t nvalue_total;      // total # of values for multi-page KMV header
    int nkey;                   // # of KMV pairs
    int nblock;                 // # of value blocks for multi-page KMV header
  };

  Page *pages;                  // list of pages
  int npage;                    // # of pages in entire KMV
  int maxpage;                  // max # of pages currently allocated

  // unique keys

  int nunique;               // current # of unique keys
  int ukeyoffset;            // offset from start of Unique to where key starts

  struct Unique {
    uint64_t nvalue;         // # of values associated with this key
    uint64_t mvbytes;        // total size of values associated with this key
    int *soffset;            // ptr to start of value sizes in KMV page
    char *voffset;           // ptr to start of values in KMV page
    Unique *next;            // ptr to next key in this hash bucket
    int keybytes;            // size of this key
    int set;                 // which KMV set this key will be part of
  };

  // hash table for unique keys

  Unique **buckets;     // ptr to 1st key in each bucket
  int hashmask;         // bit mask for mapping hashed key into hash buckets
                        // nbuckets = hashmask + 1
  uint64_t bucketbytes; // byte size of hash buckets

  char *ustart;         // ptr to where memory for Uniques starts
  char *ustop;          // ptr to where memory for Uniques stops

  // file info

  char *filename;       // filename to store KMV if needed
  FILE *fp;             // file ptr

  // partitions of KV data

  struct Partition {
    class KeyValue *kv;
    class Spool *sp;
    uint64_t nkv;
    uint64_t ksize;
    int sortbit;
  };

  Partition *partitions;
  int npartition,maxpartition;

  class Spool *seen,*unseen;
  uint64_t seen_ksize,unseen_ksize;

  // sets of unique keys

  struct Set {
    class KeyValue *kv;
    class Spool *sp;
    Unique *first;          // ptr to first Unique in set
    int nunique;            // # of Uniques in set
    int extended;           // 1 if set contains one Unique = multi-page KMV
  };

  Set *sets;
  int nset,maxset;

  // chunks of memory for Spool files

  int memspool;                 // size of memory chunk for each Spool
  char **chunks;
  int nchunk;                   // total chunks allocated

  // private methods

  void add(char *, int, char *, int);
  void collapse_one(char *, int, class KeyValue *, uint64_t);
  void collapse_many(char *, int, class KeyValue *);

  int kv2unique(int, int);
  void unseen2spools(int, int, int);
  int unique2kmv_all();
  void unique2kmv_extended(int);
  void unique2kmv_set(int);
  void unique2spools(int);
  void kv2kmv(int);
  void kv2kmv_extended(int);
  void chunk_allocate(int);

  int hash(char *, int);
  Unique *find(int, char *, int, Unique *&);

  void init_page();
  void create_page();
  void write_page();
  void read_page(int, int);
  uint64_t roundup(uint64_t, int);
};

}

#endif
