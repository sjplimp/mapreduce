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

// MapReduce random RMAT matrix generation example in C++
// Syntax: rmat N Nz a b c d frac seed {outfile}
//   2^N = # of rows in RMAT matrix
//   Nz = non-zeroes per row
//   a,b,c,d = RMAT params (must sum to 1.0)
//   frac = RMAT randomization param (frac < 1, 0 = no randomization)
//   seed = RNG seed (positive int)
//   outfile = output RMAT matrix to this filename (optional)

#include "mpi.h"
#include "math.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"

#include "blockmacros.hpp"
#include "shared.hpp"

using namespace MAPREDUCE_NS;

void generate_vertex(int, KeyValue *, void *);
void generate_edge(int, KeyValue *, void *);
void final_edge(char *, int, char *, int, int *, KeyValue *, void *);
void cull(char *, int, char *, int, int *, KeyValue *, void *);
void output(char *, int, char *, int, int *, KeyValue *, void *);
void nonzero(char *, int, char *, int, int *, KeyValue *, void *);
void degree(char *, int, char *, int, int *, KeyValue *, void *);
void histo(char *, int, char *, int, int *, KeyValue *, void *);
int ncompare(char *, int, char *, int);
void stats(uint64_t, char *, int, char *, int, KeyValue *, void *);

typedef uint64_t RMAT_VERTEX;
typedef struct RMAT_EDGE {
  RMAT_VERTEX vi, vj;
};

////////////////////////////////////////////////////////////////////////////
class GenerateRMAT{
public:
  int nlevels;          // generate 2^nlevels vertices.
  int avgdeg;           // average outdegree of vertices.
  uint64_t order;       // order = 2^nlevels
  uint64_t ngenerate;   // number of edges (nonzeros) generated so far.
  double a,b,c,d,fraction;
  char *outfile;
  FILE *fp;
  int me;
  int nprocs;
  bool printstats;

  GenerateRMAT(int, char**);
  ~GenerateRMAT() {delete [] outfile;};
  void run(MapReduce **, MapReduce **, uint64_t *, uint64_t *, uint64_t *);
};

////////////////////////////////////////////////////////////////////////////
GenerateRMAT::GenerateRMAT(int narg, char **args)
{
  int me;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  // parse command-line args

  if (me == 0) printf("Syntax for rmat: -rn N -rz z -ra a -rb b -rc c -rd d -rf frac -rs seed {-ro outfile} -rp\n");

  // Defaults
  nlevels = 1;
  avgdeg = 1;
  a = b = c = d = 0.25;
  fraction = 0.1;
  int seed = 1;
  outfile = NULL;
  printstats = false;

  int iarg = 1;

  while (iarg < narg) {
    if (strcmp(args[iarg],"-rn") == 0) {
      nlevels = atoi(args[iarg+1]); 
      iarg += 2;
    } else if (strcmp(args[iarg],"-rz") == 0) {
      avgdeg = atoi(args[iarg+1]); 
      iarg += 2;
    } else if (strcmp(args[iarg],"-ra") == 0) {
      a = atof(args[iarg+1]); 
      iarg += 2;
    } else if (strcmp(args[iarg],"-rb") == 0) {
      b = atof(args[iarg+1]); 
      iarg += 2;
    } else if (strcmp(args[iarg],"-rc") == 0) {
      c = atof(args[iarg+1]); 
      iarg += 2;
    } else if (strcmp(args[iarg],"-rd") == 0) {
      d = atof(args[iarg+1]); 
      iarg += 2;
    } else if (strcmp(args[iarg],"-rf") == 0) {
      fraction = atof(args[iarg+1]); 
      iarg += 2;
    } else if (strcmp(args[iarg],"-rs") == 0) {
      seed = atoi(args[iarg+1]); 
      iarg += 2;
    } else if (strcmp(args[iarg],"-rp") == 0) {
      printstats = true;
      iarg += 1;
    } else if (strcmp(args[iarg],"-ro") == 0) {
      int n = strlen(args[iarg+1]) + 1;
      outfile = new char[n];
      strcpy(outfile,args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-e2") == 0) {
      printf("ERROR:  -e2 not valid with rmat generation\n");
      MPI_Abort(MPI_COMM_WORLD,1);
    } else { //Skip this argument
      iarg++;
    }
  }

  if (a + b + c + d != 1.0) {
    if (me == 0) printf("ERROR: a,b,c,d must sum to 1\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (fraction >= 1.0) {
    if (me == 0) printf("ERROR: fraction must be < 1\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  srand48(seed+me);
  order = 1 << nlevels;
}

////////////////////////////////////////////////////////////////////////////
void GenerateRMAT::run(
  MapReduce **return_mrvert,   // Output:  Unique vertices
                               //          Key = Vi hashkey ID; value = NULL.
  MapReduce **return_mredge,   // Output:  Unique edges
                               //          Key = Vi hashkey ID; 
                               //          Value = {Vj hashkey ID, Wij} for
                               //          edge Vi->Vj with weight Wij
  uint64_t *nverts,            // Output:  Number of unique non-zero vertices.
  uint64_t *nrawedges,         // Output:  Number of edges in input files.
  uint64_t *nedges             // Output:  Number of unique edges in input file.
)
{
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // Generate mrvert first; it is easy.
  // Each processor generates a range of the vertices.
  MapReduce *mrvert = new MapReduce(MPI_COMM_WORLD);
  mrvert->map(nprocs, generate_vertex, this);
  mrvert->aggregate(NULL); // Not necessary, but moves vertices to procs to 
                           // which they will be hashed later.  May be good to
                           // do it once up front.
  *nverts = order;
  *return_mrvert = mrvert;

  // Now generate mredge; this is harder, as it requires the RMAT algorithm.
  MapReduce *mredge = new MapReduce(MPI_COMM_WORLD);
  mredge->verbosity = 2;
  mredge->timer = 1;

  // loop until desired number of unique nonzero entries

  int niterate = 0;
  uint64_t ntotal = (1 << nlevels) * avgdeg;
  uint64_t nremain = ntotal;
  while (nremain) {
    niterate++;
    ngenerate = nremain/nprocs;
    if (me < (nremain % nprocs)) ngenerate++;
    mredge->map(nprocs,&generate_edge,this,1);
    uint64_t nunique = mredge->collate(NULL);
    if (nunique == ntotal) break;
    mredge->reduce(&cull,NULL);
    nremain = ntotal - nunique;
  }

  // output matrix if requested

  if (outfile) {
    char fname[128];
    sprintf(fname,"%s.%04d",outfile,me);
    fp = fopen(fname,"w");
    if (fp == NULL) {
      printf("ERROR: Could not open output file");
      MPI_Abort(MPI_COMM_WORLD,1);
    }
    MapReduce *mr = mredge->copy();
    mr->reduce(&output,this);
    fclose(fp);
    delete mr;
  }

  // stats to screen
  // include stats on number of nonzeroes per row

  if (me == 0) {
    std::cout << order << " rows in matrix" << std::endl;
    std::cout << ntotal << " nonzeros in matrix" << std::endl;
  }
 
  if (printstats) {
    MapReduce *mr = mredge->copy();

    mr->reduce(&nonzero,NULL);
    mr->collate(NULL);
    mr->reduce(&degree,NULL);
    mr->collate(NULL);
    mr->reduce(&histo,NULL);
    mr->gather(1);
    mr->sort_keys(&ncompare);
    uint64_t total = 0;
    mr->map(mr,&stats,&total);
    if (me == 0) 
      std::cout << order - total << " rows with no nonzeros" << std::endl;
    delete mr;
  }

  // convert edges to correct format for return arguments
  mredge->reduce(&final_edge,NULL);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0)
    std::cout << tstop-tstart << " secs to generate matrix on " << nprocs
              << " procs in " << niterate << " iterations" << std::endl;
}

/* ----------------------------------------------------------------------
   generate RMAT matrix entries
   emit one KV per edge: key = edge, value = NULL
------------------------------------------------------------------------- */

void generate_edge(int itask, KeyValue *kv, void *ptr)
{
  GenerateRMAT *rmat = (GenerateRMAT *) ptr;

  int nlevels = rmat->nlevels;
  uint64_t order = rmat->order;
  uint64_t ngenerate = rmat->ngenerate;
  double a = rmat->a;
  double b = rmat->b;
  double c = rmat->c;
  double d = rmat->d;
  double fraction = rmat->fraction;

  uint64_t i,j,delta;
  int ilevel;
  double a1,b1,c1,d1,total,rn;
  RMAT_EDGE edge;

  for (uint64_t m = 0; m < ngenerate; m++) {
    delta = order >> 1;
    a1 = a; b1 = b; c1 = c; d1 = d;
    i = j = 0;
    
    for (ilevel = 0; ilevel < nlevels; ilevel++) {
      rn = drand48();
      if (rn < a1) {
      } else if (rn < a1+b1) {
        j += delta;
      } else if (rn < a1+b1+c1) {
        i += delta;
      } else {
        i += delta;
        j += delta;
      }
      
      delta /= 2;
      if (fraction > 0.0) {
        a1 += a1*fraction * (drand48() - 0.5);
        b1 += b1*fraction * (drand48() - 0.5);
        c1 += c1*fraction * (drand48() - 0.5);
        d1 += d1*fraction * (drand48() - 0.5);
        total = a1+b1+c1+d1;
        a1 /= total;
        b1 /= total;
        c1 /= total;
        d1 /= total;
      }
    }

    edge.vi = i;
    edge.vj = j;
    kv->add((char *) &edge,sizeof(RMAT_EDGE),NULL,0);
  }
}

/* ----------------------------------------------------------------------
   eliminate duplicate edges
   input: one KMV per edge, MV has multiple entries if duplicates exist
   output: one KV per edge: key = edge, value = NULL
------------------------------------------------------------------------- */

void cull(char *key, int keybytes, char *multivalue,
          int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,NULL,0);
}

/* ----------------------------------------------------------------------
   write edges to a file unique to this processor
------------------------------------------------------------------------- */

void output(char *key, int keybytes, char *multivalue,
            int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  GenerateRMAT *rmat = (GenerateRMAT *) ptr;
  RMAT_EDGE *edge = (RMAT_EDGE *) key;
  fprintf(rmat->fp,"%llu %llu 1\n",edge->vi+1,edge->vj+1);
}

/* ----------------------------------------------------------------------
   enumerate nonzeroes in each row
   input: one KMV per edge
   output: one KV per edge: key = row I, value = NULL
------------------------------------------------------------------------- */

void nonzero(char *key, int keybytes, char *multivalue,
             int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  RMAT_EDGE *edge = (RMAT_EDGE *) key;
  kv->add((char *) &edge->vi,sizeof(RMAT_VERTEX),NULL,0);
}

/* ----------------------------------------------------------------------
   count nonzeroes in each row
   input: one KMV per row, MV has entry for each nonzero
   output: one KV: key = # of nonzeroes, value = NULL
------------------------------------------------------------------------- */

void degree(char *key, int keybytes, char *multivalue,
         int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  uint64_t total_nvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, total_nvalues)
  kv->add((char *) &total_nvalues,sizeof(uint64_t),NULL,0);
}

/* ----------------------------------------------------------------------
   count rows with same # of nonzeroes
   input: one KMV per nonzero count, MV has entry for each row
   output: one KV: key = # of nonzeroes, value = # of rows
------------------------------------------------------------------------- */

void histo(char *key, int keybytes, char *multivalue,
           int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  uint64_t total_nvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, total_nvalues)
  kv->add(key,keybytes,(char *) &total_nvalues,sizeof(uint64_t));
}

/* ----------------------------------------------------------------------
   compare two counts
   order values by count, largest first
------------------------------------------------------------------------- */

int ncompare(char *p1, int len1, char *p2, int len2)
{
  uint64_t i1 = *(uint64_t *) p1;
  uint64_t i2 = *(uint64_t *) p2;
  if (i1 > i2) return -1;
  else if (i1 < i2) return 1;
  else return 0;
}

/* ----------------------------------------------------------------------
   print # of rows with a specific # of nonzeroes
------------------------------------------------------------------------- */

void stats(uint64_t itask, char *key, int keybytes, char *value,
           int valuebytes, KeyValue *kv, void *ptr)
{
  uint64_t *total = (uint64_t *) ptr;
  uint64_t nnz = *(uint64_t *) key;
  uint64_t ncount = *(uint64_t *) value;
  *total += ncount;
  std::cout << ncount << " rows with " << nnz << " nonzeros" << std::endl;
}

// ----------------------------------------------------------------------
// Generate vertices.
// Input:   order = total number of vertices; itask ~= processor number.
// Output:  one KV for each vertex in itask * (order / nprocs).
//----------------------------------------------------------------------- */
void generate_vertex(int itask, KeyValue *kv, void *ptr)
{
  GenerateRMAT *rmat = (GenerateRMAT *) ptr;
  uint64_t fraction = rmat->order / rmat->nprocs;
  uint64_t remainder = rmat->order % rmat->nprocs;
  RMAT_VERTEX first_vtx = 0;

  assert(itask >= 0);
  uint64_t utask = (uint64_t) itask;

  if (utask > 0) 
    first_vtx = (utask-1) * fraction + MIN((utask-1), remainder);

  RMAT_VERTEX last_vtx = first_vtx + fraction + (utask < remainder);

  for (RMAT_VERTEX i = first_vtx; i < last_vtx; i++) {
    kv->add((char *) &i, sizeof(RMAT_VERTEX), NULL, 0);
  }
}

// ----------------------------------------------------------------------
// Convert key = RMAT_EDGE to key = vertex, value = EDGE08.
// Input:   key = RMAT_EDGE
// Output:  key = RMAT_EDGE.i  value = RMAT_EDGE.j + weight
//----------------------------------------------------------------------- */
void final_edge(char *key, int keybytes, char *multivalue,
                int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  RMAT_EDGE *edge = (RMAT_EDGE *) key;
  EDGE08 edge08;
  edge08.v.v[0] = edge->vj;
  edge08.wt = 1;   // All RMAT edges are unique.
  kv->add((char *) &(edge->vi), sizeof(RMAT_VERTEX),
          (char *) &edge08, sizeof(EDGE08));
}
