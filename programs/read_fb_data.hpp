// Read Karl's link files into a graph.
// Identify unique vertices and edges.
// Options:
//         -e1
//              build graph from 1 value per edge in link file
//         -e2
//              build graph from 2 values per edge in link file
//         -gb
//              Input file is in Greg Bayer's format, with each record
//              containing:
//                 32-bit timestamp
//                 32-bit time error
//                 64-bit from host ID
//                 64-bit from host path
//                 64-bit to host ID
//                 64-bit to host path
//         -ff file
//              file with list of binary link files to read in (one per line)
//         -f file1 file2 ...
//              binary link files to read in
//              if specified, this must be the last switch used

#ifndef _READ_FB_HPP
#define _READ_FB_HPP

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.hpp"
#include "localdisks.hpp"
#include "shared.hpp"

using namespace std;
using namespace MAPREDUCE_NS;

static void readFB_fileread1(int, char *, KeyValue *, void *);
static void readFB_fileread2(int, KeyValue *, void *);
static void readFB_vertex_emit(char *, int, char *, int, int *, 
                               KeyValue *, void *);
static void readFB_vertex_unique(char *, int, char *, int, int *, 
                                 KeyValue *, void *);
template <class ReadFileType>
static void readFB_edge_unique(char *, int, char *, int, int *, 
                               KeyValue *, void *);

// Data input information.
// Standard format is 4 64-bit fields per record, for a total of 32 bytes.
// Greg Bayer's format adds 2 32-bit fields at the beginning of each record.
class ReadFBData{
public:
  ReadFBData(int narg, char **args, bool invwtflag=false);
  ~ReadFBData() {delete [] onefile;}
  void run(MapReduce **, MapReduce **, uint64_t*, uint64_t*, uint64_t*);

  int nfiles;       // If -f option is used, how many files are listed?
  char **argfiles;  // If -f option is used, argfiles points to list of files.
  char *onefile;    // If -ff option is used, onefile contains the filename.
  int GREG_BAYER;   // Offset size for Greg Bayer's format; valid values: 0 or 8
  int vertexsize;   // Use 8-bytes or 16-bytes for one vertex hashkey ID?
  const int RECORDSIZE;  // Base recordsize; always set to 32 bytes.
  const int CHUNK;       // Base chunk size.
  int me;                // Processor ID.
  double timeMap;        // Time for doing input maps.
  double timeUnique;     // Time for computing unique edges/vertices.
  bool invWt;            // Flag indicating whether or not to use 1/occurrence
                         // count as edge weight.
};

ReadFBData::ReadFBData(int narg, char *args[], bool invwtflag) : 
             onefile(NULL), GREG_BAYER(0), vertexsize(8),
             RECORDSIZE(32), CHUNK(8192), invWt(invwtflag)
{
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  int flag = 0;
  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-gb") == 0) {
      // Input file is in Greg Bayer's format.
      if (iarg+1 > narg) {
        flag = 1;
        break;
      }
      GREG_BAYER = 8;
      iarg += 1;
    } else if (strcmp(args[iarg],"-e1") == 0) {
      // Use one 64-bit fields as a vertex ID.
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
    } else if (strcmp(args[iarg],"-ff") == 0) {
      // Use one file of data filenames.
      if (iarg+2 > narg) {
        flag = 1;
        break;
      }
      int n = strlen(args[iarg+1]) + 1;
      onefile = new char[n];
      strcpy(onefile,args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-f") == 0) {
      // Use one or more data files listed here.
      if (iarg+2 > narg) {
        flag = 1;
        break;
      }
      nfiles = narg-1 - iarg; 
      argfiles = &args[iarg+1];
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

  if (onefile == NULL && argfiles == NULL) {
    if (me == 0) printf("No input files specified.\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
}

/////////////////////////////////////////////////////////////////////////////
// run:  Read the input files and generate MapReduce objects with unique
//       vertices and edges.
void ReadFBData::run(
  MapReduce **return_mrvert,   // Output:  Unique vertices
                               //          Key = Vi hashkey ID; value = NULL.
                               //          Aggregated by Vi.
  MapReduce **return_mredge,   // Output:  Unique edges
                               //          Key = Vi hashkey ID; 
                               //          Value = {Vj hashkey ID, Wij} for
                               //          edge Vi->Vj with weight Wij
                               //          Aggregated by Vi.
  uint64_t *nverts,            // Output:  Number of unique non-zero vertices.
  uint64_t *nrawedges,         // Output:  Number of edges in input files.
  uint64_t *nedges             // Output:  Number of unique edges in input file.
) 
{
  // mrraw = all edges in file data
  // pass remaining list of filenames to map()

  MapReduce *mrraw = new MapReduce(MPI_COMM_WORLD);
  mrraw->verbosity = 0;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  if (me == 0) printf("Reading input files...\n");
  if (onefile) *nrawedges = mrraw->map(onefile,&readFB_fileread1,this);
  else *nrawedges = mrraw->map(nfiles,&readFB_fileread2,this);

  MPI_Barrier(MPI_COMM_WORLD);
  double tmap = MPI_Wtime();
  timeMap = tmap - tstart;

  // mrvert = unique non-zero vertices

  if (me == 0) printf("Finding unique vertices...\n");
  MapReduce *mrvert = mrraw->copy();

  mrvert->clone();
  if (me == 0) printf("        clone complete.\n");
  mrvert->reduce(&readFB_vertex_emit,this);
  if (me == 0) printf("        vertex_emit complete.\n");
  mrvert->collate(NULL);
  if (me == 0) printf("        collate complete.\n");
  *nverts = mrvert->reduce(&readFB_vertex_unique,NULL);
  if (me == 0) printf("        vertex_unique complete.\n");

  // mredge = unique I->J edges with I and J non-zero + edge weights
  //          (computed as number of occurrences of I->J in input OR 
  //           1/number of occurrences of I->J).
  // no longer need mrraw
  if (me == 0) printf("Finding unique edges...\n");
  MapReduce *mredge = mrraw;

  mredge->collate(NULL);
  *nedges = mredge->reduce(&readFB_edge_unique<ReadFBData>,this);
  mredge->aggregate(NULL);

  *return_mrvert = mrvert;
  *return_mredge = mredge;

  MPI_Barrier(MPI_COMM_WORLD);
  timeUnique = MPI_Wtime() - tmap;

  if (me == 0) {
    cout << "ReadFB:  Number of edges:            " << *nrawedges << endl;
    cout << "ReadFB:  Number of unique edges:     " << *nedges << endl;
    cout << "ReadFB:  Number of unique vertices:  " << *nverts << endl;
  }
}

/* ----------------------------------------------------------------------
   fileread1 map() function
   for each record in file:
     vertexsize = 8: KV = 1st 8-byte field, 3rd 8-byte field
     vertexsize = 16: KV = 1st 16-byte field, 2nd 16-byte field
   output KV: ([Vi Vj], NULL)  (EDGE is key; value is NULL.)
------------------------------------------------------------------------- */

static void readFB_fileread1(int itask, char *filename, KeyValue *kv, void *ptr)
{
  ReadFBData *rfp = (ReadFBData *) ptr;
  char buf[rfp->CHUNK*(rfp->RECORDSIZE+rfp->GREG_BAYER)];

  FILE *fp = fopen(filename,"rb");
  if (fp == NULL) {
    printf("Could not open link file %s\n", filename);
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  while (1) {
    int nrecords = fread(buf,rfp->RECORDSIZE+rfp->GREG_BAYER,rfp->CHUNK,fp);
    char *ptr = buf;
    for (int i = 0; i < nrecords; i++) {
      ptr += rfp->GREG_BAYER;  // if GREG_BAYER format, skip the extra fields.
      if (rfp->vertexsize == 16) 
        kv->add(&ptr[0],2*rfp->vertexsize,NULL,0);
      else { // rfp->vertex_size == 8)
        uint64_t key[2];
        key[0] = ((uint64_t *) ptr)[0];
        key[1] = ((uint64_t *) ptr)[2];
        kv->add((char *) key, 2*sizeof(uint64_t), NULL, 0);
      }
      ptr += rfp->RECORDSIZE;
    }
    if (nrecords == 0) break;
  }

  fclose(fp);
}

/* ----------------------------------------------------------------------
   fileread2 map() function
   for each record in file:
     vertexsize = 8: KV = 1st 8-byte field, 3rd 8-byte field
     vertexsize = 16: KV = 1st 16-byte field, 2nd 16-byte field
   output KV: ([Vi Vj], NULL)  (EDGE is key; value is NULL.)
------------------------------------------------------------------------- */

static void readFB_fileread2(int itask, KeyValue *kv, void *ptr)
{
  ReadFBData *rfp = (ReadFBData *) ptr;
  char buf[rfp->CHUNK*(rfp->RECORDSIZE+rfp->GREG_BAYER)];

  char **files = rfp->argfiles;
  FILE *fp = fopen(files[itask],"rb");
  if (fp == NULL) {
    printf("Could not open link file\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  while (1) {
    int nrecords = fread(buf,rfp->RECORDSIZE+rfp->GREG_BAYER,rfp->CHUNK,fp);
    char *ptr = buf;
    for (int i = 0; i < nrecords; i++) {
      ptr += rfp->GREG_BAYER;  // if GREG_BAYER format, skip the extra fields.
      if (rfp->vertexsize == 16) {
        kv->add(&ptr[0],2*rfp->vertexsize,NULL,0);
      }
      else { // rfp->vertex_size == 8)
        uint64_t key[2];
        key[0] = ((uint64_t *) ptr)[0];
        key[1] = ((uint64_t *) ptr)[2];
        kv->add((char *) key, 2*sizeof(uint64_t), NULL, 0);
      }
      ptr += rfp->RECORDSIZE;
    }
    if (nrecords == 0) break;
  }

  fclose(fp);
}

/* ----------------------------------------------------------------------
   vertex_emit reduce() function
   input KMV: ([Vi Vj], NULL)  (Key is EDGE, Value is NULL);
   output KV: (Vi,NULL) (Vj,NULL)
   omit any Vertex if first 8 bytes is 0
------------------------------------------------------------------------- */

static void readFB_vertex_emit(char *key, int keybytes, char *multivalue,
                 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  ReadFBData *rfp = (ReadFBData *) ptr;
  uint64_t *vi = (uint64_t *) key;
  if (*vi != 0) kv->add((char *) vi,rfp->vertexsize,NULL,0);
  uint64_t *vj = (uint64_t *) (key + rfp->vertexsize);
  if (*vj != 0) kv->add((char *) vj,rfp->vertexsize,NULL,0);
}

/* ----------------------------------------------------------------------
   vertex_unique reduce() function
   input KMV: (Vi,[NULL NULL ...])
   output KV: (Vi,NULL)
------------------------------------------------------------------------- */

static void readFB_vertex_unique(char *key, int keybytes, char *multivalue,
                   int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,NULL,0);
}

/* ----------------------------------------------------------------------
   edge_unique reduce() function
   input KMV: ([Vi Vj], NULL) (Key is EDGE, Value is NULL,
                               nvalues is # of occurrences of edge)
   output KV: (Vi,{Vj,Wj}) where Wj is weight of edge Vi->Vj.
   only an edge where first 8 bytes of Vi or Vj are both non-zero is emitted
   only unique edges are emitted
------------------------------------------------------------------------- */
template <class ReadFileType>
static void readFB_edge_unique(char *key, int keybytes, char *multivalue,
                 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  ReadFileType *rfp = (ReadFileType *) ptr;
  uint64_t *vi = (uint64_t *) key;
  uint64_t *vj = (uint64_t *) (key + rfp->vertexsize);
  if (*vi == 0) return;
  if (*vj == 0) return;

  if (rfp->vertexsize == 16) {

    EDGE16 edge;
    edge.v.v[0] = vj[0];
    edge.v.v[1] = vj[1];
    if (rfp->invWt) edge.wt = 1./nvalues;
    else            edge.wt = nvalues;
    kv->add((char *) vi, rfp->vertexsize, (char *) &edge, sizeof(EDGE16));

  } else {  // vertexsize = 8

    EDGE08 edge;
    edge.v.v[0] = vj[0];
    if (rfp->invWt) edge.wt = 1./nvalues;
    else            edge.wt = nvalues;
    kv->add((char *) vi, rfp->vertexsize, (char *) &edge, sizeof(EDGE08));
  }
}

#endif
