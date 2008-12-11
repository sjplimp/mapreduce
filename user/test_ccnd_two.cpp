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

#include "test_cc_common.h"

using namespace std;
using namespace MAPREDUCE_NS;

void reduce1(char *, int, char *, int, int *, KeyValue *, void *);
void reduce2(char *, int, char *, int, int *, KeyValue *, void *);
void reduce3a(char *, int, char *, int, int *, KeyValue *, void *);
void reduce3b(char *, int, char *, int, int *, KeyValue *, void *);
void reduce4(char *, int, char *, int, int *, KeyValue *, void *);
void output_vtxstats(char *, int, char *, int, int *, KeyValue *, void *);
void output_vtxdetail(char *, int, char *, int, int *, KeyValue *, void *);
void output_zonestats(char *, int, char *, int, int *, KeyValue *, void *);
int reduce2_hash(char *, int);
int reduce3a_hash(char *, int);

/* ---------------------------------------------------------------------- */

typedef struct {
  ZONE zone;
  int row;
} REDUCE2KEY;

typedef struct {
  EDGE e;       
  ZONE zone;
  int col;
} REDUCE2VALUE;

typedef struct {
  ZONE zone;
  int col;
} REDUCE3AKEY;

typedef struct {
  EDGE e;
  ZONE zone;
} REDUCE3AVALUE;

typedef struct {
  EDGE e;
  ZONE zone;
} REDUCE3BVALUE;

typedef struct {
  VERTEX v;
  ZONE zone;
} VTXDETAIL;

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
  cc.seed = 1;

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
  cc.random = new RanMars(cc.seed+me);  // Always initialize; needed in reduce2.

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
      mr->verbosity = 1;
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

#ifdef KDDTIME
double KDDtmp = MPI_Wtime();
#endif

  mr->reduce(&reduce1,&cc);

#ifdef KDDTIME
double KDD1 = MPI_Wtime() - KDDtmp;
double KDD2c = 0, KDD2r = 0;
double KDD3c = 0, KDD3r = 0;
double KDD4c = 0, KDD4r = 0;
#endif


  int iter = 0;

  while (1) {

#ifdef KDDTIME
KDDtmp = MPI_Wtime();
#endif

    mr->collate(NULL);

#ifdef KDDTIME
KDD2c += (MPI_Wtime() - KDDtmp);
KDDtmp = MPI_Wtime();
#endif

    mr->reduce(&reduce2,&cc);

#ifdef KDDTIME
KDD2r += (MPI_Wtime() - KDDtmp);
KDDtmp = MPI_Wtime();
#endif

    nCC = mr->collate(reduce2_hash);   //  Want to hash on row here.

#ifdef KDDTIME
KDD3c += (MPI_Wtime() - KDDtmp);
KDDtmp = MPI_Wtime();
#endif

    iter++;
    if (me == 0) 
      printf("Iteration %d Number of Parts of Components = %d\n", iter, nCC);

    cc.doneflag = 1;
    mr->reduce(&reduce3a,&cc);

#ifdef KDDTIME
KDD3r += (MPI_Wtime() - KDDtmp);
KDDtmp = MPI_Wtime();
#endif

    nCC = mr->collate(reduce3a_hash);   //  Want to hash on col here.

#ifdef KDDTIME
KDD3c += (MPI_Wtime() - KDDtmp);
KDDtmp = MPI_Wtime();
#endif

    mr->reduce(&reduce3b,&cc);

#ifdef KDDTIME
KDD3r += (MPI_Wtime() - KDDtmp);
#endif


    int alldone;
    MPI_Allreduce(&cc.doneflag,&alldone,1,MPI_INT,MPI_MIN,MPI_COMM_WORLD);
    if (alldone) break;

#ifdef KDDTIME
KDDtmp = MPI_Wtime();
#endif

    mr->collate(NULL);

#ifdef KDDTIME
KDD4c += (MPI_Wtime() - KDDtmp);
KDDtmp = MPI_Wtime();
#endif

    mr->reduce(&reduce4,&cc);
#ifdef KDDTIME
KDD4r += (MPI_Wtime() - KDDtmp);
if (me == 0) printf("KDDTIME %d ONE %f  TWO (%f %f)  THREE (%f %f)  FOUR (%f %f)\n", iter, KDD1, KDD2c, KDD2r, KDD3c, KDD3r, KDD4c, KDD4r);
#endif

  }
#ifdef KDDTIME
if (me == 0) printf("KDDTIME %d ONE %f  TWO (%f %f)  THREE (%f %f)  FOUR (%f %f)\n", iter, KDD1, KDD2c, KDD2r, KDD3c, KDD3r, KDD4c, KDD4r);
#endif

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
   reduce1 function
   Input:  One KMV per vertex; MV lists all edges incident to the vertex.
   Output:  One KV per edge: key = edge e_ij; value = initial state_i
   Initial state of a vertex k is zone=k.
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE1(v, e, s) \
    printf("reduce1:  Vertex %d  Key EDGE (%d %d) Value ZONE %d\n", \
            v, e->vi, e->vj, s);  
#define HELLO_REDUCE1(v, n) \
    printf("HELLO REDUCE1 Vertex %d Nvalues %d\n", *v, nvalues);
#else
#define PRINT_REDUCE1(v, e, s)
#define HELLO_REDUCE1(v, n)
#endif

void reduce1(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  VERTEX *v = (VERTEX *) key;
  EDGE *e = (EDGE *) multivalue;
  ZONE zone;

  HELLO_REDUCE1(v, nvalues);

  zone = *v;
  for (int n = 0; n < nvalues; n++, e++) {
    kv->add((char *) e, sizeof(EDGE), (char *) &zone, sizeof(ZONE));
    PRINT_REDUCE1(*v, e, zone);
  }
}

/* ----------------------------------------------------------------------
   reduce2 function
   Input:  One KMV per edge; MV lists state_i, state_j of v_i, v_j in edge e_ij.
   Output: KV pair for each zone of the edge.
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE2(key, rval) \
    printf("reduce2:  Key (%d %d) Value [Edge (%d %d) Zone %d Col %d]\n", \
           key.zone, key.row, rval.e.vi, rval.e.vj, \
           rval.zone, rval.col);  
#define HELLO_REDUCE2(key, nvalues) \
   printf("HELLO REDUCE2  (%d %d) nvalues %d\n", \
          ((EDGE *)key)->vi, ((EDGE *)key)->vj, nvalues);
#else
#define PRINT_REDUCE2(key, rval) 
#define HELLO_REDUCE2(key, nvalues) 
#endif


void reduce2(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  HELLO_REDUCE2(key, nvalues);
  CC *cc = (CC *) ptr;

  assert(nvalues >= 2);  // For graphs, each edge has two vertices, so 
                         // the multivalue should have at least two states.
                         // Since we allow reduce4 to emit duplicates,
                         // reduce2 may receive more than 2 states.

  ZONE *si = (ZONE *) multivalue; 
  ZONE *sj = si+1;

  // We are allowing duplicate edges to be emitted by reduce4.
  // If this edge is completely within one zone, all multivalues will be the 
  // same.  But just in case the edge isn't completely within one zone, we have
  // to check duplicates for different zones.  Once we have two different
  // zones or have searched all duplicates, we can move on.
  if (*si == *sj)
    for (int i = 2; i < nvalues; i++) {
      sj = si + i;
      if (*si != *sj) break;
    }
  
  REDUCE2VALUE rval;
  REDUCE2KEY rkey;
  RanMars *random = cc->random;
  rkey.row = (int) (random->uniform() * cc->nprocs);

  rval.e = *((EDGE *) key);
  rval.col = (int) (random->uniform() * cc->nprocs);

  // Pick the better zone for this edge's vertices; better zone has lower value.
  ZONE better = *si;
  if (*sj < *si) 
    better = *sj;
  rval.zone = better;

  if (*si == *sj) {
    rkey.zone = *si;
    kv->add((char *) &rkey, sizeof(REDUCE2KEY), 
            (char *) &rval, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(rkey, rval);
  }
  else {
    rkey.zone = *si;
    kv->add((char *) &rkey, sizeof(REDUCE2KEY), 
            (char *) &rval, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(rkey, rval);

    rkey.zone = *sj;
    kv->add((char *) &rkey, sizeof(REDUCE2KEY), 
            (char *) &rval, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(rkey, rval);
  }
}

/* ----------------------------------------------------------------------
   reduce3a function
   input one KMV pair per (zone, row) containing edges in (zone, row)
   one value in multi-value = Eij, Zone_i, Col_i
     Eij = (Vi,Vj)
   output KV = edges from (zone, row) with updated best zone
     key = (zone, col), value = (Eij,Z_best_in_row)
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE3A(key, value) \
    printf("reduce3a:  Key (%d %d) Value [Edge (%d %d) Zone %d]\n", \
           key.zone, key.col, value.e.vi, value.e.vj, value.zone)
#define HELLO_REDUCE3A(key, nvalues) \
   printf("HELLO REDUCE3A  (%d %d) nvalues %d\n", key->zone, key->row,  nvalues)
#else
#define PRINT_REDUCE3A(key, value) 
#define HELLO_REDUCE3A(key, nvalues) 
#endif


void reduce3a(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  CC *cc = (CC *) ptr;
  int i;
  REDUCE2KEY *rkey = (REDUCE2KEY *) key;

  HELLO_REDUCE3A(rkey, nvalues);
  
  // Find smallest zone among all vertices in edges in this (zone.
  REDUCE2VALUE *value;
  int minzone = cc->nvtx + 1;
  for (i = 0, value = (REDUCE2VALUE*)multivalue; i < nvalues; i++, value++) {
    if (value->zone < minzone) minzone = value->zone;
  }

  if (rkey->zone != minzone) cc->doneflag = 0;

  // Relabel all vertices in (zone,row) to have minzone.
  REDUCE3AVALUE value3;
  value3.zone = minzone;

  REDUCE3AKEY rkey3;
  rkey3.zone = rkey->zone;

  for (i = 0, value = (REDUCE2VALUE*)multivalue; i < nvalues; i++, value++) {
    rkey3.col = value->col;
    value3.e = value->e;
    kv->add((char *) &rkey3, sizeof(REDUCE3AKEY), 
            (char *) &value3, sizeof(REDUCE3AVALUE));
    PRINT_REDUCE3A(rkey3, value3);
  }
}

/* ----------------------------------------------------------------------
   reduce3b function
   input one KMV pair per (zone, col) containing edges in (zone, col)
   one value in multi-value = Eij, Zone_best_in_row
     Eij = (Vi,Vj), Zone_best_in_row is best zone from row in reduce3a.
   output KV = vertices with updated state
     key = Vi, value = (Eij,Zi)
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE3B(key, value) \
    printf("reduce3b:  Key %d  Value [Edge (%d %d) Zone %d]\n", \
           key, value.e.vi, value.e.vj, value.zone)
#define HELLO_REDUCE3B(key, nvalues) \
   printf("HELLO REDUCE3B  (%d %d) nvalues %d\n", key->zone, key->col,  nvalues)
#else
#define PRINT_REDUCE3B(key, value) 
#define HELLO_REDUCE3B(key, nvalues) 
#endif

void reduce3b(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  CC *cc = (CC *) ptr;
  int i;
  REDUCE3AKEY *rkey = (REDUCE3AKEY *) key;

  HELLO_REDUCE3B(rkey, nvalues);
  
  // Find smallest zone among all vertices in edges in this zone.
  REDUCE3AVALUE *value;
  int minzone = cc->nvtx + 1;
  for (i = 0, value = (REDUCE3AVALUE*)multivalue; i < nvalues; i++, value++) {
    if (value->zone < minzone) minzone = value->zone;
  }

  if (rkey->zone != minzone) cc->doneflag = 0;

  // Relabel all vertices in zone to have minzone.
  REDUCE3BVALUE value3;
  value3.zone = minzone;

  for (i = 0, value = (REDUCE3AVALUE*)multivalue; i < nvalues; i++, value++) {

    VERTEX vi = value->e.vi;
    VERTEX vj = value->e.vj;
    value3.e = value->e;
    kv->add((char *) &vi,sizeof(VERTEX), 
            (char *) &value3,sizeof(REDUCE3BVALUE));
    PRINT_REDUCE3B(vi, value3);

    kv->add((char *) &vj,sizeof(VERTEX), 
            (char *) &value3,sizeof(REDUCE3BVALUE));
    PRINT_REDUCE3B(vj, value3);
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
    printf("reduce4:  Vertex %d  Key (%d %d) Value ZONE %d \n", \
            v, e.vi, e.vj, s);  
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
  REDUCE3BVALUE *r = (REDUCE3BVALUE *) multivalue;
  ZONE best;
  int n;

  best = r->zone;
  r++;  // Processed 0th entry already.  Move on.
  for (n = 1; n < nvalues; n++, r++) 
    if (r->zone < best) 
      best = r->zone;

  // Emit edges with updated state for vertex key.
  for (n = 0, r = (REDUCE3BVALUE *) multivalue; n < nvalues; n++, r++) {
    kv->add((char *) &(r->e), sizeof(EDGE), (char *) &best, sizeof(ZONE));
    PRINT_REDUCE4(*((VERTEX *) key), r->e, best);
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
  REDUCE3BVALUE *mv = (REDUCE3BVALUE *) multivalue;
  VTXDETAIL det;

  if (cc->outfile) {
    // Emit for gather to one processor for file output.
    const int zero=0;
    det.v = *((VERTEX *) key);
    det.zone = mv->zone;
    kv->add((char *) &zero, sizeof(zero), (char *) &det, sizeof(VTXDETAIL));
  }
  else {
    // Emit for reorg by zones to collect zone stats.
    kv->add((char *) &(mv->zone), sizeof(mv->zone), NULL, 0);
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
  VTXDETAIL *det = (VTXDETAIL *) multivalue;
  fprintf(fp, "Vtx\tZone\n");
  for (int i = 0; i < nvalues; i++, det++) {
    fprintf(fp, "%d\t%d\n", det->v, det->zone);

    // Emit for reorg by zones to collect zone stats.
    kv->add((char *) &(det->zone), sizeof(ZONE), NULL, 0);
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
   reduce2_hash function
   Function used to collate keys coming out of reduce2.
   Assign tasks to processor based on their row.
------------------------------------------------------------------------- */

int reduce2_hash(char *key, int keybytes)
{
  REDUCE2KEY *rkey = (REDUCE2KEY *) key;
  return rkey->row;
}

/* ----------------------------------------------------------------------
   reduce3a_hash function
   Function used to collate keys coming out of reduce3a.
   Assign tasks to processor based on their col.
------------------------------------------------------------------------- */

int reduce3a_hash(char *key, int keybytes)
{
  REDUCE3AKEY *rkey = (REDUCE3AKEY *) key;
  return rkey->col;
}
