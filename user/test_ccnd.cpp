// MatVec via MapReduce
// Karen Devine and Steve Plimpton, Sandia Natl Labs
// Nov 2008
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

#include "mpi.h"
#include "math.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "random_mars.h"
#include "assert.h"

#include <map>

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
void reduce1(char *, int, char *, int, int *, KeyValue *, void *);
void reduce2(char *, int, char *, int, int *, KeyValue *, void *);
void reduce3(char *, int, char *, int, int *, KeyValue *, void *);
void reduce4(char *, int, char *, int, int *, KeyValue *, void *);
void output_vtxstats(char *, int, char *, int, int *, KeyValue *, void *);
void output_vtxdetail(char *, int, char *, int, int *, KeyValue *, void *);
void output_zonestats(char *, int, char *, int, int *, KeyValue *, void *);
void procs2lattice2d(int, int, int, int, int &, int &, int &, int &);
void procs2lattice3d(int, int, int, int, int,
                     int &, int &, int &, int &, int &, int &);
void error(int, char *);
void errorone(char *);

/* ---------------------------------------------------------------------- */

#define BIGVAL 1e20;
#define IBIGVAL 0x7FFFFFFF

typedef int VERTEX;      // vertex ID

typedef struct {         // edge = 2 vertices
  VERTEX vi,vj;
} EDGE;

typedef struct {         // vertex state = vertex ID, zone ID
  VERTEX vtx;            // vertex ID (redundant?)
  int zone;              // zone this vertex is in = vertex ID of zone root
} STATE;

typedef struct {
  EDGE e;       
  STATE si;
  STATE sj;
} REDUCE2VALUE;

typedef struct {
  EDGE e;
  STATE s;
} REDUCE3VALUE;

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

  double a,b,c,d,fraction;
  int nlevels,nnonzero,seed;
  int ngenerate;
  RanMars *random;

  char **infiles;
  char *outfile;
  STATS sizeStats;
};



/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  int nVtx, nCC;  // Number of vertices and connected components

  CC cc;
  cc.me = me;
  cc.nprocs = nprocs;

  // parse command-line args

  cc.root = -1;
  cc.input = NOINPUT;
  cc.nfiles = 0;
  cc.permute = 0;
  cc.infiles = NULL;
  cc.outfile = NULL;
  cc.nvtx = 0;

  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-r") == 0) {
      if (iarg+2 > narg) error(me,"Bad arguments");
      cc.root = atoi(args[iarg+1]);
      iarg += 2;

    } else if (strcmp(args[iarg],"-o") == 0) {
      if (iarg+2 > narg) error(me,"Bad arguments");
      int n = strlen(args[iarg+1]) + 1;
      cc.outfile = new char[n];
      strcpy(cc.outfile,args[iarg+1]);
      iarg += 2;

    } else if (strcmp(args[iarg],"-t") == 0) {
      if (iarg+2 > narg) error(me,"Bad arguments");
      if (strcmp(args[iarg+1],"ring") == 0) {
        if (iarg+3 > narg) error(me,"Bad arguments");
        cc.input = RING;
        cc.nring = atoi(args[iarg+2]); 
        cc.nvtx = cc.nring;
        iarg += 3;
      } else if (strcmp(args[iarg+1],"grid2d") == 0) {
        if (iarg+4 > narg) error(me,"Bad arguments");
        cc.input = GRID2D;
        cc.nx = atoi(args[iarg+2]); 
        cc.ny = atoi(args[iarg+3]); 
        cc.nvtx = cc.nx * cc.ny;
        iarg += 4;
      } else if (strcmp(args[iarg+1],"grid3d") == 0) {
        if (iarg+5 > narg) error(me,"Bad arguments");
        cc.input = GRID3D;
        cc.nx = atoi(args[iarg+2]); 
        cc.ny = atoi(args[iarg+3]); 
        cc.nz = atoi(args[iarg+4]); 
        cc.nvtx = cc.nx * cc.ny * cc.nz;
        iarg += 5;
      } else if (strcmp(args[iarg+1],"rmat") == 0) {
        if (iarg+10 > narg) error(me,"Bad arguments");
        cc.input = RMAT;
        cc.nlevels = atoi(args[iarg+2]); 
        cc.nnonzero = atoi(args[iarg+3]); 
        cc.a = atof(args[iarg+4]); 
        cc.b = atof(args[iarg+5]); 
        cc.c = atof(args[iarg+6]); 
        cc.d = atof(args[iarg+7]); 
        cc.fraction = atof(args[iarg+8]); 
        cc.seed = atoi(args[iarg+9]); 
	cc.random = new RanMars(cc.seed+me);
        cc.nvtx = 1 << cc.nlevels;
        iarg += 10;
      } else error(me,"Bad arguments");

    } else if (strcmp(args[iarg],"-f") == 0) {
      cc.input = FILES;
      iarg++;
      while (iarg < narg) {
        if (args[iarg][0] == '-') break;
        cc.infiles = 
          (char **) realloc(cc.infiles,(cc.nfiles+1)*sizeof(char *));
        cc.infiles[cc.nfiles] = args[iarg];
        cc.nfiles++;
        iarg++;
      }
    } else if (strcmp(args[iarg],"-p") == 0) {
      if (iarg+2 > narg) error(me,"Bad arguments");
      cc.permute = atoi(args[iarg+1]);
      iarg += 2;
    } else error(me,"Bad arguments");
  }

  if (cc.input == NOINPUT) error(me,"No input specified");

  // find connected components via MapReduce

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 0;

  if (cc.input == FILES) {
    mr->map(nprocs,cc.nfiles,cc.infiles,'\n',80,&file_map1,&cc);
    int tmp = cc.nvtx;
    MPI_Allreduce(&tmp, &cc.nvtx, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

  } else if (cc.input == RMAT) {
    int ntotal = (1 << cc.nlevels) * cc.nnonzero;
    int nremain = ntotal;
    while (nremain) {
      cc.ngenerate = nremain/nprocs;
      if (me < nremain % nprocs) cc.ngenerate++;
      mr->verbosity = 2;
      mr->map(nprocs,&rmat_generate,&cc,1);
      int nunique = mr->collate(NULL);
      if (nunique == ntotal) break;
      mr->reduce(&rmat_cull,&cc);
      nremain = ntotal - nunique;
    }
    mr->reduce(&rmat_map1,&cc);
    mr->verbosity = 0;

  } else if (cc.input == RING)
    mr->map(nprocs,&ring_map1,&cc);
  else if (cc.input == GRID2D)
    mr->map(nprocs,&grid2d_map1,&cc);
  else if (cc.input == GRID3D)
    mr->map(nprocs,&grid3d_map1,&cc);

  // need to mark root vertex if specified, relabel with ID = 0 ??

  nVtx = mr->collate(NULL);
  int numSingletons = cc.nvtx - nVtx;  // Num vertices with degree zero.

  mr->reduce(&reduce1,&cc);

  int iter = 0;

  while (1) {
    mr->collate(NULL);
    mr->reduce(&reduce2,&cc);

    nCC = mr->collate(NULL);
    iter++;
    if (me == 0) printf("Iteration %d Number of Components = %d\n", iter, nCC);

    cc.doneflag = 1;
    mr->reduce(&reduce3,&cc);

    int alldone;
    MPI_Allreduce(&cc.doneflag,&alldone,1,MPI_INT,MPI_MIN,MPI_COMM_WORLD);
    if (alldone) break;

    mr->collate(NULL);
    mr->reduce(&reduce4,&cc);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  // Output some results.
  // Data in mr currently is keyed by vertex v
  // multivalue includes every edge containing v, as well as v's state.

  mr->collate(NULL);  // Collate wasn't done after reduce3 when alldone.
  mr->reduce(&output_vtxstats, &cc);
  mr->collate(NULL);

  // Write all vertices with state info to a file.
  // This operation requires all vertices to be on one processor.  
  // Don't do this for big data!

  if (cc.outfile) {
    mr->reduce(&output_vtxdetail, &cc);
    mr->collate(NULL);
  }

  // Compute min/max/avg connected-component size.

  cc.sizeStats.min = (numSingletons ? 1 : nVtx); 
  cc.sizeStats.max = 1;
  cc.sizeStats.sum = 0;
  cc.sizeStats.cnt = 0;
  for (int i = 0; i < 10; i++) cc.sizeStats.histo[i] = 0;

  mr->reduce(&output_zonestats, &cc);

  STATS gCCSize;    // global CC stats
  MPI_Allreduce(&cc.sizeStats.min, &gCCSize.min, 1, MPI_INT, MPI_MIN,
                MPI_COMM_WORLD);
  MPI_Allreduce(&cc.sizeStats.max, &gCCSize.max, 1, MPI_INT, MPI_MAX,
                MPI_COMM_WORLD);
  MPI_Allreduce(&cc.sizeStats.sum, &gCCSize.sum, 1, MPI_INT, MPI_SUM,
                MPI_COMM_WORLD);
  MPI_Allreduce(&cc.sizeStats.cnt, &gCCSize.cnt, 1, MPI_INT, MPI_SUM,
                MPI_COMM_WORLD);
  MPI_Allreduce(&cc.sizeStats.histo, &gCCSize.histo, 10, MPI_INT, MPI_SUM,
                MPI_COMM_WORLD);

  // Add in degree-zero vertices
  gCCSize.sum += numSingletons;
  gCCSize.cnt += numSingletons;
  gCCSize.histo[0] += numSingletons;

  assert(gCCSize.cnt == nCC+numSingletons);
  assert(gCCSize.max <= nVtx);

  if (me == 0) {
    printf("Number of iterations = %d\n", iter);
    printf("Number of vertices = %d\n", cc.nvtx);
    printf("Number of Connected Components = %d\n", gCCSize.cnt);
    printf("Number of Singleton Vertices = %d\n", numSingletons);
    printf("Size of Connected Components (Min, Max, Avg):  %d  %d  %f\n", 
           gCCSize.min, gCCSize.max, (float) gCCSize.sum / (float) gCCSize.cnt);
    printf("Size Histogram:  ");
    for (int i = 0; i < 10; i++) printf("%d ", gCCSize.histo[i]);
    printf("\n");
  }

  // final timing

  if (me == 0)
    printf("Time to compute CC on %d procs = %g (secs)\n",
	   nprocs,tstop-tstart);

  // clean up

  delete mr;
  delete [] cc.outfile;
  free(cc.infiles);

  MPI_Finalize();
}

/* ----------------------------------------------------------------------
   read_matrix function for map
   Read matrix-market file containing edge list.
   Assumption:  All non-zero values of matrix-market file are <= 1;
   this assumption allows us to remove the header line giving the matrix
   dimensions N M NNZ.
   For each edge e_ij, emit 2 KV: 
      key = v_i, value = e_ij
      key = v_j, value = e_ij
------------------------------------------------------------------------- */

#ifdef NOISY
#define PRINT_MAP(v, e) \
    printf("MAP:  Vertex %d  Edge (%d %d)\n", v, e.vi, e.vj);
#else
#define PRINT_MAP(v, e)
#endif

void file_map1(int itask, char *bytes, int nbytes, KeyValue *kv, void *ptr)
{
  EDGE edge;
  double nzv;

  char line[81];
  int linecnt = 0;

  CC *cc = (CC *) ptr;

  for (int k = 0; k < nbytes-1; k++) {
    line[linecnt++] = bytes[k];
    if (bytes[k] == '\n') {
      if (line[0] != '%') {  // i.e., not a comment line.
        line[linecnt] = '\0'; 
        sscanf(line, "%d %d %lf", &edge.vi, &edge.vj, &nzv);
        if (nzv <= 1.) {  // See assumption above.
          if (edge.vi != edge.vj) {
            // Self edges don't contribute to a connected-components algorithm.
            // Add only non-self edges.
            kv->add((char *)&edge.vi,sizeof(VERTEX),
                    (char *) &edge,sizeof(EDGE));
            PRINT_MAP(edge.vi, edge);
            kv->add((char *)&edge.vj,sizeof(VERTEX),
                    (char *) &edge,sizeof(EDGE));
            PRINT_MAP(edge.vj, edge);
          }
        }
        else {
          // Valid matrix entry has nzv <= 1.
          // Not general for all problems!!!!
          cc->nvtx = edge.vi;
          assert(edge.vi == edge.vj);
          printf("Skipping line with values (%d %d %f)\n", 
                 edge.vi, edge.vj, nzv);
        }
      }
      linecnt = 0;
    }
  }
}

/* ----------------------------------------------------------------------
   rmat_generate function for map
   generate an RMAT matrix
   For each edge e_ij, emit 2 KV: 
      key = v_i, value = e_ij
      key = v_j, value = e_ij
------------------------------------------------------------------------- */

void rmat_generate(int itask, KeyValue *kv, void *ptr)
{
  CC *cc = (CC *) ptr;

  double a = cc->a;
  double b = cc->b;
  double c = cc->c;
  double d = cc->d;
  double fraction = cc->fraction;
  int nlevels = cc->nlevels;
  int ngenerate = cc->ngenerate;
  RanMars *random = cc->random;

  int i,j,ilevel,delta;
  double a1,b1,c1,d1,total,rn;
  EDGE edge;
  int norder = 1 << nlevels;

  for (int m = 0; m < ngenerate; m++) {
    delta = norder >> 1;
    a1 = a; b1 = b; c1 = c; d1 = d;
    i = j = 0;
    
    for (ilevel = 0; ilevel < nlevels; ilevel++) {
      rn = random->uniform();
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
	a1 += a1*fraction * (random->uniform() - 0.5);
	b1 += b1*fraction * (random->uniform() - 0.5);
	c1 += c1*fraction * (random->uniform() - 0.5);
	d1 += d1*fraction * (random->uniform() - 0.5);
	total = a1+b1+c1+d1;
	a1 /= total;
	b1 /= total;
	c1 /= total;
	d1 /= total;
      }
    }

    edge.vi = i;
    edge.vj = j;
    kv->add((char *) &edge,sizeof(EDGE),NULL,0);
  }
}

/* ----------------------------------------------------------------------
   rmat_cull function for reduce
   Input: one KMV per edge; MV has multiple entries if RMAT edge has duplicates
   Output: one KV per edge: key = I,J; value = NULL
------------------------------------------------------------------------- */

void rmat_cull(char *key, int keybytes, char *multivalue,
	       int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,NULL,0);
}

/* ----------------------------------------------------------------------
   rmat function for reduce
   Input: one KMV per unique edge; MV is NULL
   emit the edge twice (once with each endpoint vertex)
------------------------------------------------------------------------- */

void rmat_map1(char *key, int keybytes, char *multivalue,
	       int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  EDGE *edge = (EDGE *) key;
  if (edge->vi != edge->vj) {  
    // Self-edges are irrelevant in connected components
    // Emit only non-self edges.
    kv->add((char *) &(edge->vi),sizeof(VERTEX),(char *) edge,sizeof(EDGE));
    kv->add((char *) &(edge->vj),sizeof(VERTEX),(char *) edge,sizeof(EDGE));
  }
}

/* ----------------------------------------------------------------------
   compute permutation vector
------------------------------------------------------------------------- */

void compute_perm_vec(CC *cc, VERTEX n, VERTEX **permvec)
{
  VERTEX *perm = *permvec = new VERTEX[n];
  for (VERTEX i = 0; i < n; i++) perm[i] = i+1;

  srand(1);
  double denom = RAND_MAX + 1.;
  for (VERTEX i = n; i > 0; i--) {
    VERTEX number = (VERTEX) ((double) i * (double) rand() / denom);
    VERTEX temp  = perm[number];
    perm[number] = perm[i-1];
    perm[i-1]    = temp;
  }
}

/* ----------------------------------------------------------------------
   ring function for map
   ring is periodic with Nring vertices
   vertices are numbered 1 to Nring
   partition vertices in chunks of size Nring/P per proc
   emit 2 edges for each vertex I own
   this emit each edge twice (once with each endpoint vertex)
------------------------------------------------------------------------- */

void ring_map1(int itask, KeyValue *kv, void *ptr)
{
  EDGE edge;

  CC *cc = (CC *) ptr;
  int me = cc->me;
  int nprocs = cc->nprocs;
  int nring = cc->nring;

  int first = me*nring/nprocs + 1;
  int last = (me+1)*nring/nprocs + 1;

  // Create a random permutation of vertices if requested

  VERTEX *permvec = NULL;
  if (cc->permute) compute_perm_vec(cc, nring, &permvec);

  for (int v = first; v < last; v++) {
    if (cc->permute) {
      edge.vi = permvec[v-1];
      if (v+1 <= nring) edge.vj = permvec[v];
      else edge.vj = permvec[0];
    }
    else {
      edge.vi = v;
      edge.vj = v+1;
      if (edge.vj > nring) edge.vj = 1;
    }
    kv->add((char *) &(edge.vi),sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
    kv->add((char *) &(edge.vj),sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
  }

  if (cc->permute) delete [] permvec;
}

/* ----------------------------------------------------------------------
   grid2d function for map
   2d grid is non-periodic, with Nx by Ny vertices
   vertices are numbered 1 to Nx*Ny with x varying fastest, then y
   partition vertices in 2d chunks based on 2d partition of lattice
   emit 4 edges for each vertex I own (less on non-periodic boundaries)
   this emits each edge twice (once with each endpoint vertex)
------------------------------------------------------------------------- */

void grid2d_map1(int itask, KeyValue *kv, void *ptr)
{
  int i,j,ii,jj,n;
  EDGE edge;

  CC *cc = (CC *) ptr;
  int me = cc->me;
  int nprocs = cc->nprocs;
  int nx = cc->nx;
  int ny = cc->ny;

  int nx_local,nx_offset,ny_local,ny_offset;
  procs2lattice2d(me,nprocs,nx,ny,nx_local,nx_offset,ny_local,ny_offset);

  for (i = 0; i < nx_local; i++) {
    for (j = 0; j < ny_local; j++) {
      ii = i + nx_offset;
      jj = j + ny_offset;
      n = jj*nx + ii + 1;
      edge.vi = n;
      edge.vj = n-1;
      if (ii-1 >= 0) {
        kv->add((char *) &edge.vi,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
        kv->add((char *) &edge.vj,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
      }
      edge.vj = n-nx;
      if (jj-1 >= 0)  {
        kv->add((char *) &edge.vi,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
        kv->add((char *) &edge.vj,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
      }
    }
  }
}

/* ----------------------------------------------------------------------
   grid3d function for map
   3d grid is non-periodic, with Nx by Ny by Nz vertices
   vertices are numbered 1 to Nx*Ny*Nz with x varying fastest, then y, then z
   partition vertices in 3d chunks based on 3d partition of lattice
   emit 6 edges for each vertex I own (less on non-periodic boundaries)
   this emits each edge twice (once with each endpoint vertex)
------------------------------------------------------------------------- */

void grid3d_map1(int itask, KeyValue *kv, void *ptr)
{
  int i,j,k,ii,jj,kk,n;
  EDGE edge;

  CC *cc = (CC *) ptr;
  int me = cc->me;
  int nprocs = cc->nprocs;
  int nx = cc->nx;
  int ny = cc->ny;
  int nz = cc->nz;

  int nx_local,nx_offset,ny_local,ny_offset,nz_local,nz_offset;
  procs2lattice3d(me,nprocs,nx,ny,nz,nx_local,nx_offset,
                  ny_local,ny_offset,nz_local,nz_offset);

  for (i = 0; i < nx_local; i++) {
    for (j = 0; j < ny_local; j++) {
      for (k = 0; k < nz_local; k++) {
        ii = i + nx_offset;
        jj = j + ny_offset;
        kk = k + nz_offset;
        n = kk*nx*ny + jj*nx + ii + 1;
        edge.vi = n;
        edge.vj = n-1;
        if (ii-1 >= 0) {
          kv->add((char *) &edge.vi,sizeof(VERTEX),
                  (char *) &edge,sizeof(EDGE));
          kv->add((char *) &edge.vj,sizeof(VERTEX),
                  (char *) &edge,sizeof(EDGE));
        }
        edge.vj = n-nx;
        if (jj-1 >= 0) { 
          kv->add((char *) &edge.vi,sizeof(VERTEX),
                  (char *) &edge,sizeof(EDGE));
          kv->add((char *) &edge.vj,sizeof(VERTEX),
                  (char *) &edge,sizeof(EDGE));
        }
        edge.vj = n-nx*ny;
        if (kk-1 >= 0) {
          kv->add((char *) &edge.vi,sizeof(VERTEX),
                  (char *) &edge,sizeof(EDGE));
          kv->add((char *) &edge.vj,sizeof(VERTEX),
                  (char *) &edge,sizeof(EDGE));
        }
      }
    }
  }
}

/* ----------------------------------------------------------------------
   reduce1 function
   Input:  One KMV per vertex; MV lists all edges incident to the vertex.
   Output:  One KV per edge: key = edge e_ij; value = initial state_i
   Initial state of a vertex k is zone=k.
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE1(v, e, s) \
    printf("reduce1:  Vertex %d  Key EDGE (%d %d) Value STATE (%d %d)\n", \
            v, e->vi, e->vj, s.vtx, s.zone);  
#define HELLO_REDUCE1(v, n) \
    printf("HELLO REDUCE1 Vertex %d Nvalues %d\n", *v, nvalues);
#else
#define PRINT_REDUCE1(v, e, s)
#define HELLO_REDUCE1(v, n)
#endif

void reduce1(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  struct edge *eptr;
  VERTEX *v = (VERTEX *) key;
  EDGE *e = (EDGE *) multivalue;
  STATE s;

  HELLO_REDUCE1(v, nvalues);

  s.vtx = *v;
  s.zone = *v;
  for (int n = 0; n < nvalues; n++, e++) {
    kv->add((char *) e, sizeof(EDGE), (char *) &s, sizeof(STATE));
    PRINT_REDUCE1(*v, e, s);
  }
}

/* ----------------------------------------------------------------------
   reduce2 function
   Input:  One KMV per edge; MV lists state_i, state_j of v_i, v_j in edge e_ij.
   Output: KV pair for each zone of the edge.
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE2(key, rout) \
    printf("reduce2:  Key %d Value [Edge (%d %d) State (%d %d) (%d %d)]\n", \
           key, rout.e.vi, rout.e.vj, \
           rout.si.vtx, rout.si.zone, \
           rout.sj.vtx, rout.sj.zone);  
#define HELLO_REDUCE2(key, nvalues) \
   printf("HELLO REDUCE2  (%d %d) nvalues %d\n", \
          ((EDGE *)key)->vi, ((EDGE *)key)->vj, nvalues);
#else
#define PRINT_REDUCE2(key, rout) 
#define HELLO_REDUCE2(key, nvalues) 
#endif


void reduce2(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  HELLO_REDUCE2(key, nvalues);

  assert(nvalues == 2);  // For graphs, each edge has two vertices, so 
                         // the multivalue should have at most two states.

  STATE *si = (STATE *) multivalue; 
  STATE *sj = (STATE *) (multivalue + valuebytes[0]);
  
  REDUCE2VALUE rout;

  rout.e = *((EDGE *) key);
  // Order of states s_i and s_j in multivalue is not necessarily the
  // same as in edge; make sure we get them correctly ordered here.
  if (rout.e.vi != si->vtx) {
    STATE *tmp = si;
    si = sj;
    sj = tmp;
  }
  rout.si = *si;
  rout.sj = *sj;

  if (si->zone == sj->zone) {
    kv->add((char *) &(si->zone), sizeof(si->zone), 
            (char *) &rout, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(si->zone, rout);
  }
  else {
    kv->add((char *) &(si->zone), sizeof(si->zone), 
            (char *) &rout, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(si->zone, rout);

    kv->add((char *) &(sj->zone), sizeof(sj->zone), 
            (char *) &rout, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(sj->zone, rout);
  }
}

/* ----------------------------------------------------------------------
   reduce3 function
   input KMV = all edges in zone
   one value in multi-value = Eij, Si, Sj
     Eij = (Vi,Vj), Si = (Zi), Sj = (Zj)
   output KV = vertices with updated state
     key = Vi, value = (Eij,Si)
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE3(key, value) \
    printf("reduce3:  Key %d Value [Edge (%d %d) State (%d %d)]\n", \
           key, value.e.vi, value.e.vj, \
           value.s.vtx, value.s.zone)
#define HELLO_REDUCE3(key, nvalues) \
   printf("HELLO REDUCE3  %d  nvalues %d\n", key,  nvalues)
#else
#define PRINT_REDUCE3(key, value) 
#define HELLO_REDUCE3(key, nvalues) 
#endif


void reduce3(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  CC *cc = (CC *) ptr;
  int i;
  
  // Find smallest zone among all vertices in edges in this zone.
  REDUCE2VALUE *value;
  int minzone = cc->nvtx + 1;
  for (i = 0, value = (REDUCE2VALUE*)multivalue; i < nvalues; i++, value++) {
    if (value->si.zone < minzone) minzone = value->si.zone;
    if (value->sj.zone < minzone) minzone = value->sj.zone;
  }

  // Relabel all vertices in zone to have minzone.
  for (i = 0, value = (REDUCE2VALUE*)multivalue; i < nvalues; i++, value++) {
    if (value->si.zone != minzone) {
      cc->doneflag = 0;
      value->si.zone = minzone;
    }
    if (value->sj.zone != minzone) {
      cc->doneflag = 0;
      value->sj.zone = minzone;
    }
  }

  // emit 2 KV per unique edge in MV
  // Key = Vi, Val = Eij Si
  // Key = Vj, Val = Eij Sj
  // Si,Sj are extracted from updated multivalue.
  // use edge hash table to identify unique edges
  // skip edge if already in hash, else insert edge in hash and emit KVs

  map<pair<int,int>,int> ehash;
  REDUCE3VALUE value3;
  VERTEX vi, vj;

  for (i = 0, value = (REDUCE2VALUE*)multivalue; i < nvalues; i++, value++) {
    vi = value->e.vi;
    vj = value->e.vj;
    if (ehash.find(make_pair(vi,vj)) == ehash.end()) {
      ehash.insert(make_pair(make_pair(vi,vj),0));

      value3.e = value->e;
      value3.s = value->si;
      kv->add((char *) &vi,sizeof(VERTEX), 
              (char *) &value3,sizeof(REDUCE3VALUE));
      PRINT_REDUCE3(vi, value3);

      value3.s = value->sj;
      kv->add((char *) &vj,sizeof(VERTEX), 
              (char *) &value3,sizeof(REDUCE3VALUE));
      PRINT_REDUCE3(vj, value3);
    }
  }
}

/* ----------------------------------------------------------------------
   reduce4 function
   Input:  One KMV per vertex; MV is (e_ij, state_i) for all edges incident
           to v_i.
   Output:  One KV for each edge incident to v_i, with updated state_i.
           key = e_ij; value = new state_i
------------------------------------------------------------------------- */

#ifdef NOISY
#define PRINT_REDUCE4(v, e, s) \
    printf("reduce4:  Vertex %d  Key (%d %d) Value STATE (%d %d)\n", \
            v, e.vi, e.vj, s.vtx, s.zone);  
#define HELLO_REDUCE4(key, nvalues) \
    printf("HELLO REDUCE4 Vertex %d Nvalues %d\n", *((VERTEX *)key), nvalues);
#else
#define PRINT_REDUCE4(v, e, s)
#define HELLO_REDUCE4(key, nvalues)
#endif

void reduce4(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  HELLO_REDUCE4(key, nvalues);

  // Compute best state for this vertex.
  // Best state has min zone.
  REDUCE3VALUE *r = (REDUCE3VALUE *) multivalue;
  STATE best;

  best.vtx  = *((VERTEX *) key);
  best.zone = r->s.zone;

  r++;  // Processed 0th entry already.  Move on.
  for (int n = 1; n < nvalues; n++, r++) 
    if (r->s.zone < best.zone) 
      best.zone = r->s.zone;

  // Emit edges with updated state for vertex key.
  r = (REDUCE3VALUE *) multivalue;
  map<pair<int,int>,int> ehash;

  for (int n = 0; n < nvalues; n++, r++) {
    // Emit for unique edges -- no duplicates.  
    // KDD:  Replace this map with a true hash table for better performance.
    if (ehash.find(make_pair(r->e.vi, r->e.vj)) == ehash.end()) {
      ehash.insert(make_pair(make_pair(r->e.vi, r->e.vj),0));
      kv->add((char *) &(r->e), sizeof(EDGE), (char *) &best, sizeof(STATE));
      PRINT_REDUCE4(*((VERTEX *) key), r->e, best);
    }
  }
}

/* ----------------------------------------------------------------------
   output_vtxstats function
   Input:  One KMV per vertex; MV is (e_ij, state_i) for all edges incident
           to v_i.
   Output: Two options:  
           if (cc.outfile) Emit (0, state_i) to allow printing of vertex info
           else Emit (zone, state_i) to allow collecting zone stats.
------------------------------------------------------------------------- */

void output_vtxstats(char *key, int keybytes, char *multivalue,
                     int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  CC *cc = (CC *) ptr;
  REDUCE3VALUE *mv = (REDUCE3VALUE *) multivalue;

  if (cc->outfile) {
    // Emit for gather to one processor for file output.
    const int zero=0;
    kv->add((char *) &zero, sizeof(zero), (char *) &(mv->s), sizeof(STATE));
  }
  else {
    // Emit for reorg by zones to collect zone stats.
    kv->add((char *) &(mv->s.zone), sizeof(mv->s.zone), 
            (char *) &(mv->s), sizeof(STATE));
  }
}

/* ----------------------------------------------------------------------
   output_vtxdetail function
   Input:  One KMV; key = 0; MV is state_i for all vertices v_i.
   Output: Emit (zone, state_i) to allow collecting zone stats.
------------------------------------------------------------------------- */

void output_vtxdetail(char *key, int keybytes, char *multivalue,
                     int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  FILE *fp = fopen(((CC*)ptr)->outfile, "w");
  STATE *s = (STATE *) multivalue;
  fprintf(fp, "Vtx\tZone\n");
  for (int i = 0; i < nvalues; i++, s++) {
    fprintf(fp, "%d\t%d\n", s->vtx, s->zone);

    // Emit for reorg by zones to collect zone stats.
    kv->add((char *) &(s->zone), sizeof(s->zone), (char *) s, sizeof(STATE));
  }
  fclose(fp);
}

/* ----------------------------------------------------------------------
   output_zonestats function
   Input:  One KMV per zone; MV is (state_i) for all vertices v_i in zone.
   Output: None yet.
------------------------------------------------------------------------- */

void output_zonestats(char *key, int keybytes, char *multivalue,
                      int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  CC *cc = (CC *) ptr;
  
  // Compute min/max/avg component size.
  if (nvalues > cc->sizeStats.max) cc->sizeStats.max = nvalues;
  if (nvalues < cc->sizeStats.min) cc->sizeStats.min = nvalues;
  cc->sizeStats.sum += nvalues;
  cc->sizeStats.cnt++;
  int bin = (10 * nvalues) / cc->nvtx;
  if (bin == 10) bin--;
  cc->sizeStats.histo[bin]++;
}

/* ----------------------------------------------------------------------
   assign nprocs to 2d global lattice so as to minimize perimeter per proc
------------------------------------------------------------------------- */

void procs2lattice2d(int me, int nprocs, int nx, int ny,
                     int &nx_local, int &nx_offset,
                     int &ny_local, int &ny_offset)
{
  int ipx,ipy,nx_procs,ny_procs;
  double boxx,boxy,surf;
  double bestsurf = 2 * (nx+ny);
  
  // loop thru all possible factorizations of nprocs
  // surf = perimeter of a proc sub-domain
 
 ipx = 1;
  while (ipx <= nprocs) {
    if (nprocs % ipx == 0) {
      ipy = nprocs/ipx;
      boxx = float(nx)/ipx;
      boxy = float(ny)/ipy;
      surf = boxx + boxy;
      if (surf < bestsurf) {
        bestsurf = surf;
        nx_procs = ipx;
        ny_procs = ipy;
      }
    }
    ipx++;
  }

  int iprocx = me/ny_procs;
  nx_offset = iprocx*nx/nx_procs;
  nx_local = (iprocx+1)*nx/nx_procs - nx_offset;

  int iprocy = me % ny_procs;
  ny_offset = iprocy*ny/ny_procs;
  ny_local = (iprocy+1)*ny/ny_procs - ny_offset;
}

/* ----------------------------------------------------------------------
   assign nprocs to 3d global lattice so as to minimize surf area per proc
------------------------------------------------------------------------- */

void procs2lattice3d(int me, int nprocs, int nx, int ny, int nz,
                     int &nx_local, int &nx_offset,
                     int &ny_local, int &ny_offset,
                     int &nz_local, int &nz_offset)
{
  int ipx,ipy,ipz,nx_procs,ny_procs,nz_procs,nremain;
  double boxx,boxy,boxz,surf;
  double bestsurf = 2 * (nx*ny + ny*nz + nz*nx);
  
  // loop thru all possible factorizations of nprocs
  // surf = surface area of a proc sub-domain

  ipx = 1;
  while (ipx <= nprocs) {
    if (nprocs % ipx == 0) {
      nremain = nprocs/ipx;
      ipy = 1;
      while (ipy <= nremain) {
        if (nremain % ipy == 0) {
          ipz = nremain/ipy;
          boxx = float(nx)/ipx;
          boxy = float(ny)/ipy;
          boxz = float(nz)/ipz;
          surf = boxx*boxy + boxy*boxz + boxz*boxx;
          if (surf < bestsurf) {
            bestsurf = surf;
            nx_procs = ipx;
            ny_procs = ipy;
            nz_procs = ipz;
          }
        }
        ipy++;
      }
    }
    ipx++;
  }

  int nyz_procs = ny_procs*nz_procs;
  int iprocx = (me/nyz_procs) % nx_procs;
  nx_offset = iprocx*nx/nx_procs;
  nx_local = (iprocx+1)*nx/nx_procs - nx_offset;

  int iprocy = (me/nz_procs) % ny_procs;
  ny_offset = iprocy*ny/ny_procs;
  ny_local = (iprocy+1)*ny/ny_procs - ny_offset;

  int iprocz = (me/1) % nz_procs;
  nz_offset = iprocz*nz/nz_procs;
  nz_local = (iprocz+1)*nz/nz_procs - nz_offset;
}

/* ---------------------------------------------------------------------- */

void error(int me, char *str)
{
  if (me == 0) printf("ERROR: %s\n",str);
  MPI_Abort(MPI_COMM_WORLD,1);
}

/* ---------------------------------------------------------------------- */

void errorone(char *str)
{
  printf("ERROR: %s\n",str);
  MPI_Abort(MPI_COMM_WORLD,1);
}
