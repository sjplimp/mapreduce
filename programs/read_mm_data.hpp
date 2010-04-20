// Read MatrixMarket file into a graph.
// Identify unique vertices and edges.
// Options:
//         -e1
//              build graph from 1 value per edge in link file
//         -e2
//              build graph from 2 values per edge in link file
//         -f file1 file2 ...
//              binary link files to read in
//              if specified, this must be the last switch used

#ifndef _READ_MM_HPP
#define _READ_MM_HPP

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "assert.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.hpp"
#include "shared.hpp"
#include "read_fb_data.hpp"

using namespace std;
using namespace MAPREDUCE_NS;

#define MIN(a,b) ((a) < (b) ? (a) : (b))
#define MAX(a,b) ((a) > (b) ? (a) : (b))

static void readMM_fileread(int, char *, int, KeyValue *, void *);
static void readMM_vertex_emit(int, KeyValue *, void *);

// Data input information.
class ReadMMData{
public:
  ReadMMData(int narg, char **args, bool invwtflag=false);
  ~ReadMMData() {};
  void run(MapReduce **, MapReduce **, uint64_t*, uint64_t*, uint64_t*);

  char *filename;    // If -ff option is used, onefile contains the filename.
  int vertexsize;    // Use 8-bytes or 16-bytes for one vertex hashkey ID?
  int me;                // Processor ID.
  int np;                // number of processors.
  double timeMap;        // Time for doing input maps.
  double timeUnique;     // Time for computing unique edges/vertices.
  bool invWt;            // Flag indicating whether or not to use 1/occurrence
                         // count as edge weight.
  int N;                 // MM file represents an NxN matrix.
};

ReadMMData::ReadMMData(int narg, char *args[], bool invwtflag) : 
                       vertexsize(8),
                       invWt(invwtflag),
                       N(0)
{
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &np);
  int flag = 0;
  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-e1") == 0) {
      // Use one 64-bit field as a vertex ID.
      if (iarg+1 > narg) {
        flag = 1;
        break;
      }
      vertexsize = 8;
      iarg += 1;
    } else if (strcmp(args[iarg],"-e2") == 0) {
      // Use two 64-bit fields as a vertex ID.
      if (iarg+1 > narg) {
        flag = 1;
        break;
      }
      vertexsize = 16;
      iarg += 1;
    } else if (strcmp(args[iarg],"-f") == 0) {
      // Use one data files listed here.
      if (iarg+2 > narg) {
        flag = 1;
        break;
      }
      filename = args[iarg+1];
      iarg = narg;
    } else {
      // Skip this argument.
      iarg++;
    }
  }

  if (flag) {
    if (me == 0) printf("Improper command-line argument.\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (filename == NULL) {
    if (me == 0) printf("No input file specified.\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
}

/////////////////////////////////////////////////////////////////////////////
// run:  Read the input files and generate MapReduce objects with unique
//       vertices and edges.
void ReadMMData::run(
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
  // mrraw = all edges in file data
  MapReduce *mrraw = new MapReduce(MPI_COMM_WORLD);
  mrraw->verbosity = 0;
#ifdef NEW_OUT_OF_CORE
  mrraw->set_fpath((char *) MYLOCALDISK);
  mrraw->memsize = MRMEMSIZE;
#endif

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  if (me == 0) printf("Reading input files %s...\n", filename);
  *nrawedges = mrraw->map(np,1,&filename,'\n',80,&readMM_fileread,this);
  int tmp = N;
  MPI_Allreduce(&tmp, &N, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

  MPI_Barrier(MPI_COMM_WORLD);
  double tmap = MPI_Wtime();
  timeMap = tmap - tstart;

  // mrvert = unique non-zero vertices

  if (me == 0) printf("Finding unique vertices...\n");
  MapReduce *mrvert = new MapReduce(MPI_COMM_WORLD);
  *nverts = mrvert->map(np,&readMM_vertex_emit,this);
  mrvert->aggregate(NULL);

  if (me == 0) printf("        vertex_unique complete.\n");

  // mredge = unique I->J edges with I and J non-zero + edge weights
  //          (computed as number of occurrences of I->J in input OR 
  //           1/number of occurrences of I->J).
  // no longer need mrraw
  if (me == 0) printf("Finding unique edges...\n");
  MapReduce *mredge = mrraw;

  mredge->collate(NULL);
  // Use same reducer as FB data.
  *nedges = mredge->reduce(&readFB_edge_unique<ReadMMData>,this);  
  mredge->aggregate(NULL);

  *return_mrvert = mrvert;
  *return_mredge = mredge;

  MPI_Barrier(MPI_COMM_WORLD);
  timeUnique = MPI_Wtime() - tmap;

  if (me == 0) {
    cout << "ReadMM:  Number of edges:            " << *nrawedges << endl;
    cout << "ReadMM:  Number of unique edges:     " << *nedges << endl;
    cout << "ReadMM:  Number of unique vertices:  " << *nverts << endl;
  }
}

/* ----------------------------------------------------------------------
   mm_vertex_emit map() function
   Must be called with number of tasks == number of processors.
   input KMV: ReadMMData object (in ptr).  Each processor emits some of
              the vertices in range [1:N].
   output KV: (Vi,NULL) for some range of vertices in [1:N].
------------------------------------------------------------------------- */

static void readMM_vertex_emit(int itask, KeyValue *kv, void *ptr) 
{
  ReadMMData *rfp = (ReadMMData *) ptr;
  // Compute range for this task to emit.
  int ndiv = rfp->N/rfp->np;
  int nrem = rfp->N%rfp->np;
  int nemit = ndiv + (rfp->me >= nrem ? 0 : 1);
  int minv = 1 + rfp->me * ndiv + MIN(rfp->me, nrem);
  int maxv = minv + nemit;
  uint64_t v[2] = {0, 0};

  for (int i = minv; i < maxv; i++) {
    v[0] = i;
    kv->add((char *) v, rfp->vertexsize, NULL, 0);
  }
}

/* ----------------------------------------------------------------------
   readMM_fileread function for map
   Read matrix-market file containing edge list.
   Assumption:  All non-zero values of matrix-market file are <= 1;
   this assumption allows us to remove the header line giving the matrix
   dimensions N M NNZ.
   output KV: ([Vi Vj], NULL)  (EDGE is key; value is NULL.)
------------------------------------------------------------------------- */

#ifdef NOISY
#define PRINT_MAP(e, vsize) \
    (vsize == 16  \
     ? printf("MAP:  Edge (%ld %ld  -> %ld %ld) \n", e[0], e[1], e[2], e[3]) \
     : printf("MAP:  Edge (%ld  -> %ld) \n", e[0], e[1]));
#else
#define PRINT_MAP(e, vsize)
#endif


static void readMM_fileread(int itask, char *bytes, int nbytes, 
                            KeyValue *kv, void *ptr)
{
  ReadMMData *rfp = (ReadMMData *) ptr;
  uint64_t edge[4] = {0, 0, 0, 0};
  int vi, vj;
  double nzv;

  char line[81];
  int linecnt = 0;

  int vjidx = 1;
  if (rfp->vertexsize == 16) vjidx = 2;

  for (int k = 0; k < nbytes-1; k++) {
    line[linecnt++] = bytes[k];
    if (bytes[k] == '\n') {
      if (line[0] != '%') {  // i.e., not a comment line.
        line[linecnt] = '\0';
        sscanf(line, "%d %d %lf", &vi, &vj, &nzv);
        if (nzv <= 1.) {  // Assuming value is less than one to distinguish 
                          // from header line.
          edge[0] = vi;
          edge[vjidx] = vj;
          kv->add((char *)edge, 2*rfp->vertexsize, NULL, 0);
          PRINT_MAP(edge, rfp->vertexsize);
        }
        else {
          // Header line found.
          // Valid matrix entry has nzv <= 1.
          // Not general for all problems!!!!
          assert(vi == vj);  // Square matrix
          rfp->N = vi;
          printf("Skipping line with values (%d %d %f)\n",
                 vi, vj, nzv);
        }
      }
      linecnt = 0;
    }
  }
}

#endif
