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
#include "ccnd.h"

using namespace std;
using namespace MAPREDUCE_NS;

void output_vtxstats(char *, int, char *, int, int *, KeyValue *, void *);
void output_vtxdetail(char *, int, char *, int, int *, KeyValue *, void *);
void output_zonestats(char *, int, char *, int, int *, KeyValue *, void *);

/* ---------------------------------------------------------------------- */

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
  cc.twophase = 1;

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
    } else if (strcmp(args[iarg],"-z") == 0) {
      if (iarg+2 > narg) error(me,"Bad arguments");
      cc.twophase = atoi(args[iarg+1]);
      iarg += 2;
    } else error(me,"Bad arguments");
  }

  if (cc.input == NOINPUT) error(me,"No input specified");
  cc.random = new RanMars(cc.seed+me);  // Always initialize; needed in reduce2.

  // find connected components via MapReduce

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

  // Start timer now; shouldn't include I/O time in reported time.
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  int numSingletons;
  ConnectedComponentsNoDistances(mr, &cc, &numSingletons);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  // Output some results.
  // Data in mr currently is keyed by vertex v
  // multivalue includes every edge containing v, as well as v's state.

  mr->collate(NULL);  // Collate wasn't done after reduce3 when alldone.
  mr->reduce(&output_vtxstats, &cc);

  // Write all vertices with state info to a file.
  // This operation requires all vertices to be on one processor.  
  // Don't do this for big data!

  if (cc.outfile) {
    mr->collate(NULL);
    mr->reduce(&output_vtxdetail, &cc);
  }

  // Compute min/max/avg connected-component size.

  cc.sizeStats.min = (numSingletons ? 1 : cc.nvtx); 
  cc.sizeStats.max = 1;
  cc.sizeStats.sum = 0;
  cc.sizeStats.cnt = 0;
  for (int i = 0; i < 10; i++) cc.sizeStats.histo[i] = 0;

  mr->collate(NULL);
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

  if (me == 0) {
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
   output_vtxstats function
   Input:  One KMV per vertex; MV is (e_ij, state_i) for all edges incident
           to v_i.
   Output: Two options:  
           if (cc.outfile) Emit (0, state_i) to allow printing of vertex info
           else Emit (zone, NULL) to allow collecting zone stats.
------------------------------------------------------------------------- */

void output_vtxstats(char *key, int keybytes, char *multivalue,
                     int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  CC *cc = (CC *) ptr;
  EDGEZONE *mv = (EDGEZONE *) multivalue;
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
   Output: Emit (zone, NULL) to allow collecting zone stats.
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
   Input:  One KMV per zone; MV is NULL; the data we want is nvalues.
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
