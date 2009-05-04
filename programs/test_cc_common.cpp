// Connected Components via MapReduce.
// Karen Devine and Steve Plimpton, Sandia Natl Labs
// Nov-Dec 2008
//
// Shared parts of all our connected components MapReduce codes.
// Includes test-problem generation and file reading.

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


/* ----------------------------------------------------------------------
   file_map1 function for map
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
