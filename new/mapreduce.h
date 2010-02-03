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

#ifndef MAP_REDUCE_H
#define MAP_REDUCE_H

#include "mpi.h"
#include "stdint.h"

namespace MAPREDUCE_NS {

class MapReduce {
 public:
  int mapstyle;     // 0 = chunks, 1 = strided, 2 = master/slave
  int verbosity;    // 0 = none, 1 = totals, 2 = proc histograms
  int timer;        // 0 = none, 1 = summary, 2 = proc histograms
  int memsize;      // # of Mbytes for each KV and KMV
  int keyalign;     // align keys to this byte count
  int valuealign;   // align values to this byte count
  char *fpath;

  class KeyValue *kv;              // single KV stored by MR
  class KeyMultiValue *kmv;        // single KMV stored by MR

  static MapReduce *mrptr;         // holds a ptr to MR currently being used
  static int instances_now;        // total # of MRs currently instantiated
                                   // grows as created, shrinks as destroyed
  static int instances_ever;       // total # of MRs ever instantiated
                                   // grows as created, never shrinks
  static int mpi_finalize_flag;    // 1 if MR library should finalize MPI
  static uint64_t cssize,crsize;   // total send/recv bytes for all comm

  // library API

  MapReduce(MPI_Comm);
  MapReduce();
  MapReduce(double);
  ~MapReduce();

  MapReduce *copy();

  uint64_t add(MapReduce *);
  uint64_t aggregate(int (*)(char *, int));
  uint64_t clone();
  uint64_t collapse(char *, int);
  uint64_t collate(int (*)(char *, int));
  uint64_t compress(void (*)(char *, int, char *,
			     int, int *, class KeyValue *, void *),
		    void *);
  uint64_t convert();
  uint64_t gather(int);

  uint64_t map(int, void (*)(int, class KeyValue *, void *),
	       void *, int addflag = 0);
  uint64_t map(char *, void (*)(int, char *, class KeyValue *, void *),
	       void *, int addflag = 0);
  uint64_t map(int, int, char **, char, int, 
	       void (*)(int, char *, int, class KeyValue *, void *),
	       void *, int addflag = 0);
  uint64_t map(int, int, char **, char *, int, 
	       void (*)(int, char *, int, class KeyValue *, void *),
	       void *, int addflag = 0);
  uint64_t map(MapReduce *, void (*)(int, char *, int, char *, int, 
				     class KeyValue *, void *),
	       void *, int addflag = 0);

  uint64_t reduce(void (*)(char *, int, char *,
			   int, int *, class KeyValue *, void *),
		  void *);
  uint64_t scrunch(int, char *, int);

  int multivalue_blocks();
  int multivalue_block(int, char **, int **);

  uint64_t sort_keys(int (*)(char *, int, char *, int));
  uint64_t sort_values(int (*)(char *, int, char *, int));
  uint64_t sort_multivalues(int (*)(char *, int, char *, int));

  void kv_stats(int);
  void kmv_stats(int);
  void cummulative_stats(int, int);

  void set_fpath(char *);

  // query functions

  MPI_Comm communicator() {return comm;};
  int num_procs() {return nprocs;};
  int my_proc() {return me;};

  // functions accessed thru non-class wrapper functions

  void map_file_wrapper(int, class KeyValue *);
  int compare_wrapper(int, int);

 private:
  MPI_Comm comm;
  int me,nprocs;
  int instance_me;         // which instances_ever I am
  int allocated;
  double time_start,time_stop;
  class Memory *memory;
  class Error *error;

  uint64_t rsize_one,wsize_one;     // file read/write bytes for one operation
  uint64_t crsize_one,cssize_one;   // send/recv comm bytes for one operation

  // memory partitions

  char *memblock;        // memsize block of memory for KVs and KMVs
  char *mem0,*mem1;      // ptrs to first/second quarter of memblock
  char *mem2;            // remaining half of memblock
  char *memavail;        // ptr to which of mem0/mem1 is available
  int memtoggle;         // 0 if mem0 is available, 1 if mem1
  uint64_t memfull;      // size of full memory block
  uint64_t memhalf;      // size of half of memory block
  uint64_t memquarter;   // size of quarter of memory block

  int twolenbytes;                 // byte length of two ints
  int kalign,valign;               // finalized alignments for keys/value
  int talign;                      // alignment of entire KV or KMV pair
  int kalignm1,valignm1,talignm1;  // alignments-1 for masking

  // sorting

  typedef int (CompareFunc)(char *, int, char *, int);
  CompareFunc *compare;

  char *sptr;
  int *soffset,*slength;

  // storage for reduce/compress of multi-block KMVs

  int blockvalid;
  int nblock_kmv;
  int block_header_page;

  // file map()

  typedef void (MapFileFunc)(int, char *, int, class KeyValue *, void *);

  struct FileMap {
    int sepwhich;
    char sepchar;
    char *sepstr;
    int delta;
    char **filename;          // names of files to read
    uint64_t *filesize;       // size in bytes of each file
    int *tasksperfile;        // # of map tasks for each file
    int *whichfile;           // which file each map task reads
    int *whichtask;           // which sub-task in file each map task is
    MapFileFunc *appmapfile;  // user map function
    void *appptr;             // user data ptr
  };
  FileMap filemap;

  // private functions

  void defaults();
  void copy_kv(KeyValue *);
  void copy_kmv(KeyMultiValue *);
  void allocate();
  void memswap(char *);

  uint64_t map_file(int, int, char **,
		    void (*)(int, char *, int, class KeyValue *, void *),
		    void *, int addflag);

  void sort_kv(int);
  void merge(int, class Spool *, class Spool *, class Spool *);
  int extract(int, char *, char *&, int &);

  void stats(const char *, int);
  void histogram(int, double *, double &, double &, double &,
		 int, int *, int *);
  char *file_create(int, int);
  void file_stats(int);
  int roundup(int, int);
  void start_timer();
};

}

#endif
