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
  uint64_t rsize;                  // total bytes read from file
  uint64_t wsize;                  // total bytes written to file
  int fileflag;                    // 1 if file exists, 0 if not

  uint64_t spool_rsize;            // total bytes read from spool files
  uint64_t spool_wsize;            // total bytes written to spool files
  
  KeyMultiValue(MPI_Comm, char *, int, int, int, int);
  ~KeyMultiValue();

  void copy(KeyMultiValue *);
  void complete();
  int request_info(char **);
  int request_page(int, int, int &, int &, int &);
  void overwrite_page(int);
  void close_file();

  void clone(class KeyValue *);
  void collapse(char *, int, class KeyValue *);
  void convert(class KeyValue *, char *, int);

 private:
  int me;
  MPI_Comm comm;
  class Memory *memory;
  class Error *error;

  int kalign,valign;                 // alignment for keys & multivalues
  int talign;                        // alignment of entire KMV pair
  int kalignm1,valignm1,talignm1;    // alignments-1 for masking
  int twolenbytes;                   // size of key & value lengths
  int threelenbytes;                 // size of key & value & nvalue lengths

  // in-memory page

  int nkey;                     // # of KMV pairs in page
  int nvalue;                   // # of values in multivalues in page
  int keysize;                  // exact size of key data in page
  int valuesize;                // exact size of multivalue data in page
  int alignsize;                // current size of page with alignment

  char *page;                   // in-memory page
  int pagesize;                 // size of page

  // virtual pages

  struct Page {
    int nkey;                   // # of KV pairs
    int keysize;                // exact size of keys 
    int valuesize;              // exact size of multivalues
    int exactsize;              // exact size of all data in page
    int alignsize;              // rounded-up exactsize with alignment
    int filesize;               // rounded-up alignsize for file I/O
    uint64_t fileoffset;        // summed filesize of all previous pages
  };

  Page *pages;                  // list of pages
  int npage;                    // # of pages in entire KV
  int maxpage;                  // max # of pages currently allocated

  // unique keys

  struct Unique {
    int keyoffset;      // offset in ukeys of this key
    int keybytes;       // size of this key
    int soffset;        // offset to start of value sizes
    int voffset;        // offset to start of values
    int nvalue;         // # of values associated with this key
    int mvbytes;        // total size of values associated with this key
    int iset;
    int next;           // index in uniques of next key in this hash bucket
  };

  Unique *uniques;      // list of data for unique keys
  int nunique;          // current # of unique keys
  int maxunique;        // max # of unique keys that can be held in Uniques
  char *ukeys;          // unique keys, one after the other
  int ukeyoffset;       // current size of all keys in ukeys
  int maxukeys;         // max size of all keys in ukeys

  // hash table for unique keys

  int *buckets;         // index of 1st Unique entry in each bucket
  int nbuckets;         // # of hash buckets
  int hashmask;         // bit mask for mapping into hash buckets

  // file info

  char filename[32];    // filename to store KV if needed
  FILE *fp;             // file ptr

  // partitions of KV data

  struct Partition {
    uint64_t nkv;
    int ksize;
    int sortbit;
    class KeyValue *kv;
    class Spool *sp;
  };

  Partition *partitions;
  int npartition,maxpartition;

  Spool *seen,*unseen;
  int seen_ksize,unseen_ksize;

  // sets of unique keys

  struct Set {
    int first,last;
    int extended;
    class KeyValue *kv;
    class Spool *sp;
  };

  Set *sets;
  int nset,maxset;

  // chunks of memory for Spool files

  int memspool;                 // size of memory chunk for each Spool
  char **chunks;
  int nchunk;                   // total chunks allocated

  // private methods

  void add(char *, int, char *, int);

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
  int find(int, char *, int, int &);

  void init_page();
  void create_page();
  void write_page();
  void read_page(int, int);
  int roundup(int,int);
};

}

#endif
