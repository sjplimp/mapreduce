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
#include "shared.hpp"

using namespace std;
using namespace MAPREDUCE_NS;

void fileread1(int, char *, KeyValue *, void *);
void fileread2(int, KeyValue *, void *);
void vertex_emit(char *, int, char *, int, int *, KeyValue *, void *);
void vertex_unique(char *, int, char *, int, int *, KeyValue *, void *);
void edge_unique(char *, int, char *, int, int *, KeyValue *, void *);

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
  // pass remaining list of filenames to map()

  MapReduce *mrraw = new MapReduce(MPI_COMM_WORLD);
  mrraw->verbosity = 0;

#ifdef NEW_OUT_OF_CORE
  mrraw->memsize = 1024;
#endif
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  if (me == 0) printf("Reading input files...\n");
  if (onefile) *nrawedges = mrraw->map(onefile,&fileread1,this);
  else *nrawedges = mrraw->map(nfiles,&fileread2,this);

  MPI_Barrier(MPI_COMM_WORLD);
  double tmap = MPI_Wtime();
  timeMap = tmap - tstart;

  // mrvert = unique non-zero vertices

  if (me == 0) printf("Finding unique vertices...\n");
#ifdef NEW_OUT_OF_CORE
  MapReduce *mrvert = mrraw->copy();
#else
  MapReduce *mrvert = new MapReduce(*mrraw);
#endif

  mrvert->clone();
  mrvert->reduce(&vertex_emit,this);
  mrvert->collate(NULL);
  *nverts = mrvert->reduce(&vertex_unique,NULL);

  // mredge = unique I->J edges with I and J non-zero + edge weights
  //          (computed as number of occurrences of I->J in input OR 
  //           1/number of occurrences of I->J).
  // no longer need mrraw
  if (me == 0) printf("Finding unique edges...\n");
  MapReduce *mredge = mrraw;

  mredge->collate(NULL);
  *nedges = mredge->reduce(&edge_unique,this);

  *return_mrvert = mrvert;
  *return_mredge = mredge;

  MPI_Barrier(MPI_COMM_WORLD);
  timeUnique = MPI_Wtime() - tmap;

  if (me == 0) {
    printf("ReadFB:  Number of edges:            %ld\n", *nrawedges);
    printf("ReadFB:  Number of unique edges:     %ld\n", *nedges);
    printf("ReadFB:  Number of unique vertices:  %ld\n", *nverts);
  }
}

/* ----------------------------------------------------------------------
   fileread1 map() function
   for each record in file:
     vertexsize = 8: KV = 1st 8-byte field, 3rd 8-byte field
     vertexsize = 16: KV = 1st 16-byte field, 2nd 16-byte field
   output KV: (Vi,Vj)
------------------------------------------------------------------------- */

void fileread1(int itask, char *filename, KeyValue *kv, void *ptr)
{
  ReadFBData *rfb = (ReadFBData *) ptr;
  char buf[rfb->CHUNK*(rfb->RECORDSIZE+rfb->GREG_BAYER)];

  FILE *fp = fopen(filename,"rb");
  if (fp == NULL) {
    printf("Could not open link file %s\n", filename);
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  while (1) {
    int nrecords = fread(buf,rfb->RECORDSIZE+rfb->GREG_BAYER,rfb->CHUNK,fp);
    char *ptr = buf;
    for (int i = 0; i < nrecords; i++) {
      ptr += rfb->GREG_BAYER;  // if GREG_BAYER format, skip the extra fields.
      kv->add(&ptr[0],rfb->vertexsize,&ptr[16],rfb->vertexsize);
      ptr += rfb->RECORDSIZE;
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
   output KV: (Vi,Vj)
------------------------------------------------------------------------- */

void fileread2(int itask, KeyValue *kv, void *ptr)
{
  ReadFBData *rfb = (ReadFBData *) ptr;
  char buf[rfb->CHUNK*(rfb->RECORDSIZE+rfb->GREG_BAYER)];

  char **files = rfb->argfiles;
  FILE *fp = fopen(files[itask],"rb");
  if (fp == NULL) {
    printf("Could not open link file\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  while (1) {
    int nrecords = fread(buf,rfb->RECORDSIZE+rfb->GREG_BAYER,rfb->CHUNK,fp);
    char *ptr = buf;
    for (int i = 0; i < nrecords; i++) {
      ptr += rfb->GREG_BAYER;  // if GREG_BAYER format, skip the extra fields.
      kv->add(&ptr[0],rfb->vertexsize,&ptr[16],rfb->vertexsize);
      ptr += rfb->RECORDSIZE;
    }
    if (nrecords == 0) break;
  }

  fclose(fp);
}

/* ----------------------------------------------------------------------
   vertex_emit reduce() function
   input KMV: (Vi,[Vj])
   output KV: (Vi,NULL) (Vj,NULL)
   omit any Vi if first 8 bytes is 0
------------------------------------------------------------------------- */

void vertex_emit(char *key, int keybytes, char *multivalue,
                 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  ReadFBData *rfb = (ReadFBData *) ptr;
  uint64_t *vi = (uint64_t *) key;
  if (*vi != 0) kv->add((char *) vi,rfb->vertexsize,NULL,0);
  if (!multivalue) {
    printf("Error in vertex_emit; not ready for out of core. %d\n", nvalues);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  uint64_t *vj = (uint64_t *) multivalue;
  if (*vj != 0) kv->add((char *) vj,rfb->vertexsize,NULL,0);
}

/* ----------------------------------------------------------------------
   vertex_unique reduce() function
   input KMV: (Vi,[NULL NULL ...])
   output KV: (Vi,NULL)
------------------------------------------------------------------------- */

void vertex_unique(char *key, int keybytes, char *multivalue,
                   int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,NULL,0);
}

/* ----------------------------------------------------------------------
   edge_unique reduce() function
   input KMV: (Vi,[Vj Vk ...])
   output KV: (Vi,{Vj,Wj}) (Vi,{Vk,Wk}) ...
   where Wj is weight of edge Vi->Vj.
   only an edge where first 8 bytes of Vi or Vj are both non-zero is emitted
   only unique edges are emitted
------------------------------------------------------------------------- */
void edge_unique(char *key, int keybytes, char *multivalue,
                 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  ReadFBData *rfb = (ReadFBData *) ptr;
  uint64_t *vi = (uint64_t *) key;
  if (*vi == 0) return;

  if (rfb->vertexsize == 16) {

    map<std::pair<uint64_t, uint64_t>,WEIGHT> hash;

    CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)
    BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

    // Use a hash table to count the number of occurrences of each edge.
    int offset = 0;
    for (int i = 0; i < nvalues; i++) {
      uint64_t *v1 = (uint64_t *) &multivalue[offset];
      uint64_t *v2 = (uint64_t *) &multivalue[offset+8];
      if (hash.find(std::make_pair(*v1,*v2)) == hash.end())
        hash[std::make_pair(*v1,*v2)] = 1;
      else
        hash[std::make_pair(*v1,*v2)]++;
      offset += valuebytes[i];
    }

    END_BLOCK_LOOP

    // Iterate over the hash table to emit edges with their counts.
    // Note:  Assuming the hash table fits in memory!
    map<std::pair<uint64_t, uint64_t>,WEIGHT>::iterator mit;
    for (mit = hash.begin(); mit != hash.end(); mit++) {
      std::pair<uint64_t, uint64_t> e = (*mit).first;
      EDGE16 tmp;
      tmp.v.v[0] = e.first;
      tmp.v.v[1] = e.second;
      if (rfb->invWt) tmp.wt = 1./(*mit).second;
      else            tmp.wt = (*mit).second;
      kv->add(key,keybytes,(char *)&tmp,sizeof(EDGE16));
    }
  } else {  // vertexsize = 8

    map<uint64_t,WEIGHT> hash;

    CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)
    BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

    // Use a hash table to count the number of occurrences of each edge.
    uint64_t *vertex = (uint64_t *) multivalue;
    for (int i = 0; i < nvalues; i++) {
      if (vertex[i] == 0) continue;
      if (hash.find(vertex[i]) == hash.end()) 
        hash[vertex[i]] = 1;
      else
        hash[vertex[i]]++;
    }

    END_BLOCK_LOOP

    // Iterate over the hash table to emit edges with their counts.
    // Note:  Assuming the hash table fits in memory!
    map<uint64_t,WEIGHT>::iterator mit;
    for (mit = hash.begin(); mit != hash.end(); mit++) {
      EDGE08 tmp;
      tmp.v.v[0] = (*mit).first;
      if (rfb->invWt) tmp.wt = 1./(*mit).second;
      else            tmp.wt = (*mit).second;
      kv->add(key,keybytes,(char*)&tmp,sizeof(EDGE08));
    }
  }
}

#endif
