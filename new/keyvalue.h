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

#ifndef KEY_VALUE_H
#define KEY_VALUE_H

#include "mpi.h"
#include "stdio.h"
#include "stdint.h"
#include "spool.h"

namespace MAPREDUCE_NS {

class KeyValue {
  friend class MapReduce;

 public:
  uint64_t nkv;                      // # of KV pairs in entire KV on this proc
  uint64_t ksize;                    // exact size of all key data
  uint64_t vsize;                    // exact size of all value data
  uint64_t tsize;                    // total exact size of entire KV
  uint64_t rsize;                    // total bytes read from file
  uint64_t wsize;                    // total bytes written to file
  static double trsize;        // total bytes read from file for all KeyValues
  static double twsize;        // total bytes written to file for all KeyValues
  int fileflag;                      // 1 if file exists, 0 if not

  KeyValue(MPI_Comm, char *, int, int, int, int, int);
  ~KeyValue();

  void copy(KeyValue *);
  void append();
  void complete();
  int request_info(char **);
  int request_page(int, int &, int &, int &);

  void add(char *, int, char *, int);
  void add(int, char *, int, char *, int);
  void add(int, char *, int *, char *, int *);

 private:
  MPI_Comm comm;
  class Memory *memory;
  class Error *error;

  int kalign,valign;                // alignment for keys & values
  int talign;                       // alignment of entire KV pair
  int kalignm1,valignm1,talignm1;   // alignments-1 for masking
  int twolenbytes;                  // size of key & value lengths

  // in-memory page

  int nkey;                     // # of KV pairs in page
  int keysize;                  // exact size of key data in page
  int valuesize;                // exaact size of value data in page
  int alignsize;                // current size of page with alignment

  char *page;                   // in-memory page
  int pagesize;                 // size of page

  // virtual pages

  struct Page {
    int nkey;                   // # of KV pairs
    int keysize;                // exact size of keys 
    int valuesize;              // exact size of values
    int exactsize;              // exact size of all data in page
    int alignsize;              // rounded-up exactsize with alignment
    int filesize;               // rounded-up alignsize for file I/O
    uint64_t fileoffset;        // summed filesize of all previous pages
  };

  Page *pages;                  // list of pages
  int npage;                    // # of pages in entire KV
  int maxpage;                  // max # of pages currently allocated

  // file info

  char filename[MRMPI_FILENAMESIZE];       // filename to store KV if needed
  FILE *fp;                     // file ptr

  // private methods

  void add(KeyValue *);
  void add(int, char *);
  void add(int, char *, int, int, int);
  void add(int, char *, int, int);

  void init_page();
  void create_page();
  void write_page();
  void read_page(int, int);
  int roundup(int,int);
};

}

#endif
