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

#ifndef SPOOL_H
#define SPOOL_H

#include "stdio.h"

namespace MAPREDUCE_NS {

class Spool {
 public:
  uint64_t nkv;                      // # of KV entries in entire spool file
  uint64_t esize;                    // size of all entries (with alignment)

  static uint64_t rsize,wsize;       // total file r/w bytes for all SPs
  static double rtime,wtime;         // total file r/w time for all SPs

  Spool(char *, int, class Memory *, class Error *);
  ~Spool();

  void assign(char *);
  void complete();
  int request_info(char **);
  int request_page(int);
  void add(int, char *);

 private:
  class Memory *memory;
  class Error *error;

  // in-memory page

  int nentry;                   // # of entries
  int size;                     // current size of entries
  char *page;                   // in-memory page
  int pagesize;                 // size of page

  // virtual pages

  struct Page {
    int nentry;                 // # of entries
    int size;                   // size of entries
    int filesize;               // rounded-up size for file I/O
  };

  Page *pages;                  // list of pages in Spool
  int npage;                    // # of pages in Spool
  int maxpage;                  // max # of pages currently allocated

  // file info

  char *filename;               // filename to store Spool if needed
  int fileflag;                 // 1 if file exists, 0 if not
  FILE *fp;                     // file ptr

  // private methods

  void create_page();
  void write_page();
  void read_page(int);
  int roundup(int,int);
};

}

#endif
