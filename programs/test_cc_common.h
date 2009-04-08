// MatVec via MapReduce
// Karen Devine and Steve Plimpton, Sandia Natl Labs
// Dec 2008
//
// Identify connected components in a graph via MapReduce
// algorithm due to Jonathan Cohen.
// The algorithm treats all edges as undirected edges.
// 
// Syntax: concomp switch args switch args ...
// switches:
//   -r N = define N as root vertex, compute all distances from it
//   -o file = output to this file, else no output except screen summary
//   -t style params = input from a test problem
//      style params = ring N = 1d ring with N vertices
//      style params = 2d Nx Ny = 2d grid with Nx by Ny vertices
//      style params = 3d Nx Ny Nz = 3d grid with Nx by Ny by Nz vertices
//      style params = rmat N Nz a b c d frac seed
//        generate an RMAT matrix with 2^N rows, Nz non-zeroes per row,
//        a,b,c,d = RMAT params, frac = RMAT randomize param, seed = RNG seed
//   -f file1 file2 ... = input from list of files containing sparse matrix
//   -p 0/1 = turn random permutation of input data off/on (default = off)

#ifndef __TEST_CC_COMMON_H
#define __TEST_CC_COMMON_H

#include "mpi.h"
#include "math.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "random_mars.h"
#include "assert.h"

using namespace std;
using namespace MAPREDUCE_NS;

#define MAXLINE 256
#ifndef MAX
#define MAX(a, b) ((a) >= (b) ? (a) : (b))
#endif
#ifndef MIN
#define MIN(a, b) ((a) <= (b) ? (a) : (b))
#endif

enum{NOINPUT,RING,GRID2D,GRID3D,FILES,RMAT};

void file_map1(int, char *, int, KeyValue *, void *);
void rmat_generate(int, KeyValue *, void *);
void rmat_cull(char *, int, char *, int, int *, KeyValue *, void *);
void rmat_map1(char *, int, char *, int, int *, KeyValue *, void *);
void ring_map1(int, KeyValue *, void *);
void grid2d_map1(int, KeyValue *, void *);
void grid3d_map1(int, KeyValue *, void *);
void procs2lattice2d(int, int, int, int, int &, int &, int &, int &);
void procs2lattice3d(int, int, int, int, int, int &, int &, int &, int &, 
                                              int &, int &);

void error(int, char *);
void errorone(char *);

/* ---------------------------------------------------------------------- */

#define BIGVAL 1e20;
#define IBIGVAL 0x7FFFFFFF

typedef int VERTEX;      // vertex ID
typedef VERTEX ZONE;     // Zone number.
typedef struct {         // edge = 2 vertices
  VERTEX vi,vj;
} EDGE;

typedef struct {
  EDGE e;
  ZONE zone;
} EDGEZONE;

struct STATS {
  int min;
  int max;
  int sum;
  int cnt;
  int histo[10];
};

struct CC {
  int me,nprocs;
  int doneflag;
  int root;
  int input;
  int nring;
  int nx,ny,nz;
  int nfiles;
  int nvtx;
  int permute;
  int badflag;
  int twophase;   // Flag indicating whether to use one-phase or two-phase
                  // reduce3 in ccnd algorithm.
  double a,b,c,d,fraction;
  int nlevels,nnonzero,seed;
  int ngenerate;
  RanMars *random;

  char **infiles;
  char *outfile;
  STATS sizeStats;
  STATS distStats;
};


#endif
