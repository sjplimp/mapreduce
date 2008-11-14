// MatVec via MapReduce
// Karen Devine and Steve Plimpton, Sandia Natl Labs
// Nov 2008
//
// Identify connected components in a graph via MapReduce
// algorithm due to Jonathan Cohen
// 
// Syntax: cc switch args switch args ...
// switches:
//   -r N = define N as root vertex, compute all distances from it
//   -o file = output to this file, else no output except screen summary
//   -t style params = input from a test problem
//      style params = ring N = 1d ring with N vertices
//      style params = 2d Nx Ny = 2d grid with Nx by Ny vertices
//      style params = 3d Nx Ny Nz = 3d grid with Nx by Ny by Nz vertices
//   -f file1 file2 ... = input from list of files containing sparse matrix

#include "mpi.h"
#include "math.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "assert.h"

using namespace MAPREDUCE_NS;

#define MAXLINE 256
#ifndef MAX
#define MAX(a, b) ((a) >= (b) ? (a) : (b))
#endif
#ifndef MIN
#define MIN(a, b) ((a) <= (b) ? (a) : (b))
#endif

enum{NOINPUT,RING,GRID2D,GRID3D,FILES};

void read_matrix(int, char *, int, KeyValue *, void *);
void ring(int, KeyValue *, void *);
void grid2d(int, KeyValue *, void *);
void grid3d(int, KeyValue *, void *);
void reduce1(char *, int, char *, int, int *, KeyValue *, void *);
void reduce2(char *, int, char *, int, int *, KeyValue *, void *);
void reduce3(char *, int, char *, int, int *, KeyValue *, void *);
void reduce4(char *, int, char *, int, int *, KeyValue *, void *);
int sort(char *, int, char *, int);
void procs2lattice2d(int, int, int, int, int &, int &, int &, int &);
void procs2lattice3d(int, int, int, int, int,
		     int &, int &, int &, int &, int &, int &);
void error(int, char *);
void errorone(char *);

struct CC {
  int me,nprocs;
  int doneflag;
  int root;
  int input;
  int nring;
  int nx,ny,nz;
  int nfiles;
  char **infiles;
  char *outfile;
};

/* ---------------------------------------------------------------------- */
typedef int VERTEX;   //  Data type for vertices.
#define BIGVAL 1e20;

typedef struct {
  VERTEX vi, vj;
} EDGE;

typedef struct {
  VERTEX vtx;  // Vertex ID
  int zone; // Current zone in state
  int dist; // Distance from root of zone
} STATE;

typedef struct {   // Describes struct of value emitted from reduce 2.
  float sortdist;  // Distance used for sorting in reduce3.
  EDGE e;       
  STATE si;
  STATE sj;
} REDUCE2VALUE;

typedef struct {   // Describes struct of value emitted from reduce 3.
  EDGE e;
  STATE s;
} REDUCE3VALUE;

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
  cc.infiles = NULL;
  cc.outfile = NULL;

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
	iarg += 3;
      } else if (strcmp(args[iarg+1],"grid2d") == 0) {
	if (iarg+4 > narg) error(me,"Bad arguments");
	cc.input = GRID2D;
	cc.nx = atoi(args[iarg+2]); 
	cc.ny = atoi(args[iarg+3]); 
	iarg += 4;
      } else if (strcmp(args[iarg+1],"grid3d") == 0) {
	if (iarg+5 > narg) error(me,"Bad arguments");
	cc.input = GRID3D;
	cc.nx = atoi(args[iarg+2]); 
	cc.ny = atoi(args[iarg+3]); 
	cc.nz = atoi(args[iarg+4]); 
	iarg += 5;
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
    } else error(me,"Bad arguments");
  }

  if (cc.input == NOINPUT) error(me,"No input specified");

  // find connected components via MapReduce

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 2;

  if (cc.input == FILES)
    mr->map(nprocs,cc.nfiles,cc.infiles,'\n',80,&read_matrix,&cc);
  else if (cc.input == RING)
    mr->map(nprocs,&ring,&cc);
  else if (cc.input == GRID2D)
    mr->map(nprocs,&grid2d,&cc);
  else if (cc.input == GRID3D)
    mr->map(nprocs,&grid3d,&cc);

  // need to mark root vertex if specified, relabel with ID = 0 ??

  mr->collate(NULL);
  mr->reduce(&reduce1,&cc);

  while (1) {
    mr->collate(NULL);
    mr->reduce(&reduce2,&cc);

    mr->collate(NULL);
    cc.doneflag = 1;
    mr->reduce(&reduce3,&cc);

    int alldone;
    MPI_Allreduce(&cc.doneflag,&alldone,1,MPI_INT,MPI_MIN,MPI_COMM_WORLD);
    if (alldone) break;

    mr->collate(NULL);
    mr->reduce(&reduce4,&cc);
  }

  delete mr;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  // statistics
  // test answer if test problem was used
  // do more output if -o flag was specified

  if (me == 0)
    printf("Time to compute CC on %d procs = %g (secs)\n",nprocs,tstop-tstart);

  // clean up

  delete [] cc.outfile;
  free(cc.infiles);

  MPI_Finalize();
}

/* ----------------------------------------------------------------------
   read_matrix function for map
------------------------------------------------------------------------- */

void read_matrix(int itask, char *str, int size, KeyValue *kv, void *ptr)
{
  char *line = strtok(str,"\n");
  while (line) {
    // parse line and emit KVs
    line = strtok(NULL,"\n");
  }
}

/* ----------------------------------------------------------------------
   ring function for map
   ring is periodic with Nring vertices
   vertices are numbered 1 to Nring
   partition vertices in chunks of size Nring/P per proc
   emit 2 edges for each vertex I own; 
   emit each edge twice (once with each endpoint vertex).
------------------------------------------------------------------------- */

void ring(int itask, KeyValue *kv, void *ptr)
{
  EDGE edge;

  CC *cc = (CC *) ptr;
  int me = cc->me;
  int nprocs = cc->nprocs;
  int nring = cc->nring;

  int first = me*nring/nprocs + 1;
  int last = (me+1)*nring/nprocs + 1;

  for (int v = first; v < last; v++) {
    edge.vi = v;
    edge.vj = v+1;
    if (edge.vj > nring) edge.vj = 1;
    kv->add((char *) &(edge.vi),sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
    kv->add((char *) &(edge.vj),sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
    edge.vj = v-1;
    if (edge.vj == 0) edge.vj = nring;
    kv->add((char *) &(edge.vi),sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
    kv->add((char *) &(edge.vj),sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
  }
}

/* ----------------------------------------------------------------------
   grid2d function for map
   2d grid is non-periodic, with Nx by Ny vertices
   vertices are numbered 1 to Nx*Ny with x varying fastest, then y
   partition vertices in 2d chunks based on 2d partition of lattice
   emit 4 edges for each vertex I own (less on non-periodic boundaries);
   emit each edge twice (once with each endpoint vertex).
------------------------------------------------------------------------- */

void grid2d(int itask, KeyValue *kv, void *ptr)
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
      edge.vj = n+1;
      if (ii+1 < nx) {
	kv->add((char *) &edge.vi,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
	kv->add((char *) &edge.vj,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
      }
      edge.vj = n-nx;
      if (jj-1 >= 0)  {
	kv->add((char *) &edge.vi,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
	kv->add((char *) &edge.vj,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
      }
      edge.vj = n+nx;
      if (jj+1 < ny) {
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
   emit 6 edges for each vertex I own (less on non-periodic boundaries);
   emit each edge twice (once with each endpoint vertex).
------------------------------------------------------------------------- */

void grid3d(int itask, KeyValue *kv, void *ptr)
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
	  kv->add((char *) &edge.vi,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
	  kv->add((char *) &edge.vj,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
        }
	edge.vj = n+1;
	if (ii+1 < nx) {
	  kv->add((char *) &edge.vi,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
	  kv->add((char *) &edge.vj,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
        }
	edge.vj = n-nx;
	if (jj-1 >= 0) { 
	  kv->add((char *) &edge.vi,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
	  kv->add((char *) &edge.vj,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
        }
	edge.vj = n+nx;
	if (jj+1 < ny) {
	  kv->add((char *) &edge.vi,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
	  kv->add((char *) &edge.vj,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
        }
	edge.vj = n-nx*ny;
	if (kk-1 >= 0) {
	  kv->add((char *) &edge.vi,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
	  kv->add((char *) &edge.vj,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
        }
	edge.vj = n+nx*ny;
	if (kk+1 < nz) {
	  kv->add((char *) &edge.vi,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
	  kv->add((char *) &edge.vj,sizeof(VERTEX),(char *) &edge,sizeof(EDGE));
        }
      }
    }
  }
}

/* ----------------------------------------------------------------------
   reduce1 function
   Input:  One KMV per vertex; MV lists all edges incident to the vertex.
   Output:  One KV per edge: key = edge e_ij; value = initial state_i
   Initial state of a vertex k is zone=k, dist=0.
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE1(e, s) \
    printf("reduce1:  Key (%d %d) Value (%d %d %d)\n", \
            e->vi, e->vj, s.vtx, s.zone, s.dist);  
#define HELLO_REDUCE1(v, n) \
    printf("HELLO REDUCE1 Vertex %d Nvalues %d\n", *v, nvalues);
#else
#define PRINT_REDUCE1(e, s)
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
  s.dist = 0;
  for (int n = 0; n < nvalues; n++, e++) {
    kv->add((char *) e, sizeof(EDGE), (char *) &s, sizeof(STATE));
    PRINT_REDUCE1(e, s);
  }
}

/* ----------------------------------------------------------------------
   reduce2 function
   Input:  One KMV per edge; MV lists state_i, state_j of v_i, v_j in edge e_ij.
   Output:  Up to three KV based on state_i, state_j of v_i, v_j in edge e_ij.
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE2(key, rout) \
    printf("reduce2:  Key %d Value [%f (%d %d) (%d %d %d) (%d %d %d)]\n", \
           key, rout.sortdist, rout.e.vi, rout.e.vj, \
           rout.si.vtx, rout.si.zone, rout.si.dist, \
           rout.sj.vtx, rout.sj.zone, rout.sj.dist);  
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
  
  float dmin = MIN(si->dist, sj->dist);
  float dmax = MAX(si->dist, sj->dist);
  int zmax = MAX(si->zone, sj->zone);

  REDUCE2VALUE rout;

  rout.e = *((EDGE *) key);
  rout.si = *si;
  rout.sj = *sj;

  if (si->zone == sj->zone) {
    rout.sortdist = dmin;
    kv->add((char *) &(si->zone), sizeof(si->zone), 
            (char *) &rout, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(si->zone, rout);

    rout.sortdist = -(dmax + (dmax - dmin) / (dmax + 1));
    kv->add((char *) &(si->zone), sizeof(si->zone), 
            (char *) &rout, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(si->zone, rout);
  }
  else {
    rout.sortdist = dmin;
    kv->add((char *) &(si->zone), sizeof(si->zone), 
            (char *) &rout, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(si->zone, rout);

    kv->add((char *) &(sj->zone), sizeof(sj->zone), 
            (char *) &rout, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(sj->zone, rout);

    rout.sortdist = -BIGVAL;
    kv->add((char *) &zmax, sizeof(zmax), 
            (char *) &rout, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(zmax, rout);
  }
}

/* ----------------------------------------------------------------------
   reduce3 function
   input KMV = all edges in zone, stored twice with different D values
   one value in multi-value = B, Eij, Si, Sj
     B = sorting criterion, Eij = (Vi,Vj), Si = (Zi,dist), Sj = (Zj,dist)
   output KV = vertices with updated state
     key = Vi, value = (Eij,Si)
------------------------------------------------------------------------- */

void reduce3(char *key, int keybytes, char *multivalue,
	      int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  CC *cc = (CC *) ptr;
  
  // create empty hash table for vertex states
  // key = vertex ID
  // value = vertex state = Si = (Zi,dist) where Zi = zone ID

  // load hash table with vertices of all edges in multi-value

  // sort multi-values by B
  // create index vector

  // loop over edges in sorted order
  // extract Si and Sj for Eij from hash table
  // Zmin = min(Zi,Zj)
  // Dmin = lowest dist of vertex whose S has Zmin
  // if Si or Sj is already (Zmin,Dmin), don't change it
  // if Si or Sj is not (Zmin,Dmin), change it to Snew = (Zmin,Dmin+1)
  // is Si changes, put it back in hash table, set cc.doneflag = 1

  // create 2nd hash table to store unique Eij in multi-value

  // emit 2 KV per unique edge in MV, skip edge if already in hash table
  // Key = Vi, Val = Eij Si    (Use data type REDUCE3VALUE for Val.)
  // Key = Vj, Val = Eij Sj    (Use data type REDUCE3VALUE for Val.)
  // Si,Sj are extracted from hash table

  // delete 2 hash tables
}

/* ----------------------------------------------------------------------
   reduce4 function
   Input:  One KMV per vertex; MV is (e_ij, state_i) for all edges incident
           to v_i.
   Output:  One KV for each edge indicent to v_i, with updated state_i.
           key = e_ij; value = new state_i
------------------------------------------------------------------------- */

#ifdef NOISY
#define PRINT_REDUCE4(e, s) \
    printf("reduce4:  Key (%d %d) Value (%d %d %d)\n", \
            e.vi, e.vj, s.vtx, s.zone, s.dist);  
#define HELLO_REDUCE4(key, nvalues) \
    printf("HELLO REDUCE4 Vertex %d Nvalues %d\n", *((VERTEX *)key), nvalues);
#else
#define PRINT_REDUCE4(e, s)
#define HELLO_REDUCE4(key, nvalues)
#endif

void reduce4(char *key, int keybytes, char *multivalue,
	      int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  // Compute best state for this vertex.
  // Best state has min zone, then min dist.
  REDUCE3VALUE *r = (REDUCE3VALUE *) multivalue;
  STATE best;

  best.zone = r->s.zone;
  best.dist = r->s.dist;

  r++;  // Processed 0th entry already.  Move on.
  for (int n = 1; n < nvalues; n++, r++) {
    if (r->s.zone < best.zone) {
      best.zone = r->s.zone;
      best.dist = r->s.dist;
    }
    else if (r->s.zone == best.zone)
      best.dist = MIN(r->s.dist, best.dist);
  }

  // Emit 
  r = (REDUCE3VALUE *) multivalue;
  for (int n = 0; n < nvalues; n++, r++) {
    kv->add((char *) &(r->e), sizeof(EDGE), (char *) &best, sizeof(STATE));
    PRINT_REDUCE4(r->e, best);
  }
}

/* ----------------------------------------------------------------------
   sort function for sorting KMV values
------------------------------------------------------------------------- */

int sort(char *p1, int len1, char *p2, int len2)
{
  int i1 = *(int *) p1;
  int i2 = *(int *) p2;
  if (i1 < i2) return -1;
  else if (i1 > i2) return 1;
  else return 0;
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
