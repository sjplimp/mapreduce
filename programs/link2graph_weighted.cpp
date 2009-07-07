// convert Karl's link files into a graph
// print stats on resulting graph
// optionally output graph as Matrix Market file

// Syntax: link2graph switch arg(s) switch arg(s) ...
//         -c
//              convert hashed vertex IDs to integer vertex IDs (1 to N)
//         -e1
//              build graph from 1 value per edge in link file
//         -e2
//              build graph from 2 values per edge in link file
//         -out histofile
//              print vertex out-degree histogramming to histofile
//         -in histofile
//              print vertex in-degree histogramming to histofile
//         -m matrixfile
//              print graph in Matrix Market format with 1/outdegree field
//         -mw
//              print edge weight instead of 1/outdegree field in Matrix Market
//         -h hashfile
//              print graph edges as (i,j) pairs using original hashvalues
//              from input file (not converted to [1:N]).  Works for -e1 or -e2.
//         -ff file
//              file with list of binary link files to read in (one per line)
//         -f file1 file2 ...
//              binary link files to read in
//              if specified, this must be the last switch used

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"

#include <map>

using namespace std;
using namespace MAPREDUCE_NS;

void fileread1(int, char *, KeyValue *, void *);
void fileread2(int, KeyValue *, void *);
void vertex_emit(char *, int, char *, int, int *, KeyValue *, void *);
void vertex_unique(char *, int, char *, int, int *, KeyValue *, void *);
void edge_unique(char *, int, char *, int, int *, KeyValue *, void *);
void vertex_label(char *, int, char *, int, int *, KeyValue *, void *);
void edge_label1(char *, int, char *, int, int *, KeyValue *, void *);
void edge_label2(char *, int, char *, int, int *, KeyValue *, void *);
void edge_count(char *, int, char *, int, int *, KeyValue *, void *);
void edge_reverse(char *, int, char *, int, int *, KeyValue *, void *);
void edge_histo(char *, int, char *, int, int *, KeyValue *, void *);
int histo_sort(char *, int, char *, int);
void histo_write(char *, int, char *, int, int *, KeyValue *, void *);
void hfile_write(char *, int, char *, int, int *, KeyValue *, void *);
void matrix_write_inverse_degree(char *, int, char *, int, int *, KeyValue *, void *);
void matrix_write_weights(char *, int, char *, int, int *, KeyValue *, void *);

// Vertex type in 1-N ordering; occasionally we use negative values for
// identifying special cases, so make sure this type is signed.
typedef int VERTEX;

typedef struct {
  VERTEX nthresh;
  VERTEX count;
} LABEL;

// Edge Weight type; set to number of occurrence of edge in original input.
typedef unsigned int WEIGHT;

// Edge with destination vertex (16-bytes) and edge weight
// Note:  edge_label1 assumes v is first field of this struct.
typedef struct {
  uint64_t v[2];
  WEIGHT wt;  
} EDGE16;

// Edge with destination vertex (8-bytes) and edge weight
// Note:  edge_label1 assumes v is first field of this struct.
typedef struct {
  uint64_t v[1];
  WEIGHT wt;  
} EDGE08;

// Edge with destination vertex (VERTEXTYPE) and edge weight
typedef struct {
  VERTEX v;
  WEIGHT wt;
} EDGE;

#define RECORDSIZE 32
#define CHUNK 8192

static int vertexsize = 8;

#ifdef NEW_OUT_OF_CORE

// Macro defining how to loop over blocks when multivalue is stored in
// more than one block.  This macro is used frequently, so we define it here.
// Code to be executed on each block should be between a BEGIN_BLOCK_LOOP
// and an END_BLOCK_LOOP.
// Note:  This mechanism is a little clunky.  Make sure you DO NOT have a 
// semicolon afer these macros.

#define BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues) { \
  int bbb_nblocks = 1; \
  MapReduce *bbb_mr = NULL; \
  if (!(multivalue)) { \
    bbb_mr = (MapReduce *) (valuebytes); \
    bbb_nblocks = bbb_mr->multivalue_blocks(); \
  } \
  for (int bbb_iblock = 0; bbb_iblock < bbb_nblocks; bbb_iblock++) { \
    if (bbb_mr)  \
      (nvalues) = bbb_mr->multivalue_block(bbb_iblock, \
                                           &(multivalue),&(valuebytes)); 

#define BREAK_BLOCK_LOOP break
#define END_BLOCK_LOOP } }

#else  // !NEW_OUT_OF_CORE

// These macros are not needed with the in-core mapreduce library.
// We'll define them as no-ops (with curly braces so compilation is consistent,
// though, so we don't need as many #ifdefs in the code.

#define BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues) {

#define BREAK_BLOCK_LOOP 
#define END_BLOCK_LOOP }

#endif  // NEW_OUT_OF_CORE

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  // parse command-line args

  char *outhfile = NULL;
  char *inhfile = NULL;
  char *mfile = NULL;
  bool mfile_weights = 0;
  char *hfile = NULL;
  int convertflag = 0;
  char *onefile = NULL;
  int nfiles = 0;
  char **argfiles = NULL;

  int flag = 0;
  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-out") == 0) {
      // Generate an outdegree histogram.
      if (iarg+2 > narg) {
	flag = 1;
	break;
      }
      int n = strlen(args[iarg+1]) + 1;
      outhfile = new char[n];
      strcpy(outhfile,args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-in") == 0) {
      // Generate an indegree histogram.
      if (iarg+2 > narg) {
	flag = 1;
	break;
      }
      int n = strlen(args[iarg+1]) + 1;
      inhfile = new char[n];
      strcpy(inhfile,args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-m") == 0) {
      // Generate a matrix-market output file.
      if (iarg+2 > narg) {
	flag = 1;
	break;
      }
      int n = strlen(args[iarg+1]) + 1;
      mfile = new char[n];
      strcpy(mfile,args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-mw") == 0) {
      // Use edge weights as values in matrix-market file.
      if (iarg+1 > narg) {
	flag = 1;
	break;
      }
      mfile_weights = 1;
      iarg += 1;
    } else if (strcmp(args[iarg],"-h") == 0) {
      // Generate a output file of edges using hashvalues from input file.
      if (iarg+2 > narg) {
	flag = 1;
	break;
      }
      int n = strlen(args[iarg+1]) + 1;
      hfile = new char[n];
      strcpy(hfile,args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-c") == 0) {
      // Convert vertex IDs to range 1-N.
      if (iarg+1 > narg) {
	flag = 1;
	break;
      }
      convertflag = 1;
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
      flag = 1;
      break;
    }
  }

  if (flag) {
    if (me == 0)
      printf("Syntax: link2graph switch arg(s) switch arg(s) ...\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (onefile == NULL && argfiles == NULL) {
    if (me == 0) printf("No input files specified");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (convertflag == 0 && (outhfile || inhfile)) {
    if (me == 0) printf("Must convert vertex values if histogram\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (convertflag == 0 && mfile) {
    if (me == 0) printf("Must convert vertex values if output matrix\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  // process the files and create a graph/matrix

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // mrraw = all edges in file data
  // pass remaining list of filenames to map()

  MapReduce *mrraw = new MapReduce(MPI_COMM_WORLD);
  mrraw->verbosity = 1;

#ifdef NEW_OUT_OF_CORE
  mrraw->memsize = 1;
#endif

  if (me == 0) printf("Reading input files...\n");
  int nrawedges;
  if (onefile) nrawedges = mrraw->map(onefile,&fileread1,NULL);
  else nrawedges = mrraw->map(nfiles,&fileread2,argfiles);

  // mrvert = unique non-zero vertices

  if (me == 0) printf("Finding unique vertices...\n");
#ifdef NEW_OUT_OF_CORE
  MapReduce *mrvert = mrraw->copy();
#else
  MapReduce *mrvert = new MapReduce(*mrraw);
#endif

  mrvert->clone();
  mrvert->reduce(&vertex_emit,NULL);
  mrvert->collate(NULL);
  int nverts = mrvert->reduce(&vertex_unique,NULL);

  // mredge = unique I->J edges with I and J non-zero + edge weights
  //          (computed as number of occurrences of I->J in input).
  // no longer need mrraw
  if (me == 0) printf("Finding unique edges...\n");
#ifdef NEW_OUT_OF_CORE
  MapReduce *mredge = mrraw->copy();
#else
  MapReduce *mredge = new MapReduce(*mrraw);
#endif

  mredge->collate(NULL);
  int nedges = mredge->reduce(&edge_unique,NULL);
  delete mrraw;

  // set nsingleton to -1 in case never compute it via options

  int nsingleton = -1;

  // output a hash-key file of unique edges I->J.
  // No header information; one chunk per proc.
  // Must do this before the convert, as convert re-writes mredges.
  if (hfile) {

    if (me == 0) {
      printf("Printing hash-key files ...\n");
      char fname[128];
      sprintf(fname,"%s.header",hfile);
      FILE *fp = fopen(fname,"w");
      fprintf(fp,"%d %d %d\n",nverts,nverts,nedges);
      fclose(fp);
    }
    
    char fname[128];
    sprintf(fname,"%s.%d",hfile,me);
    FILE *fp = fopen(fname,"w");

    // mrout KV = (Vi,[Vj Vk ... Vz ])
    // print out edges in using hashvalues from input file.
    
#ifdef NEW_OUT_OF_CORE
    MapReduce *mrout = mredge->copy();
#else
    MapReduce *mrout = new MapReduce(*mredge);
#endif

    mrout->convert();
    mrout->reduce(&hfile_write,fp);

    delete mrout;

    fclose(fp);
  }

  // update mredge so its vertices are unique ints from 1-N, not hash values

  if (convertflag) {

    // mrvertlabel = vertices with unique IDs 1-N
    // label.nthresh = # of verts on procs < me
    // no longer need mrvert

    if (me == 0) printf("Converting hash-keys to integers...\n");
    LABEL label;
    label.count = 0;

#ifdef NEW_OUT_OF_CORE
    int nlocal = mrvert->kv->nkv;
#else
    int nlocal = mrvert->kv->nkey;
#endif

    MPI_Scan(&nlocal,&label.nthresh,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
    label.nthresh -= nlocal;

#ifdef NEW_OUT_OF_CORE
    MapReduce *mrvertlabel = mrvert->copy();
#else
    MapReduce *mrvertlabel = new MapReduce(*mrvert);
#endif

    mrvertlabel->clone();
    mrvertlabel->reduce(&vertex_label,&label);
    
    delete mrvert;

    // reset all vertices in mredge from 1 to N

#ifdef NEW_OUT_OF_CORE
    mredge->add(mrvertlabel);
#else
    mredge->kv->add(mrvertlabel->kv);
#endif

    mredge->collate(NULL);
    mredge->reduce(&edge_label1,NULL);

#ifdef NEW_OUT_OF_CORE
    mredge->add(mrvertlabel);
#else
    mredge->kv->add(mrvertlabel->kv);
#endif

    mredge->collate(NULL);
    mredge->reduce(&edge_label2,NULL);
    
    delete mrvertlabel;
  } else delete mrvert;

  // compute and output an out-degree histogram

  if (outhfile) {

    // mrdegree = vertices with their out degree as negative value
    // nsingleton = # of verts with 0 outdegree
    
    if (me == 0) printf("Generating out-degree histogram...\n");
#ifdef NEW_OUT_OF_CORE
    MapReduce *mrdegree = mredge->copy();
#else
    MapReduce *mrdegree = new MapReduce(*mredge);
#endif

    mrdegree->collate(NULL);
    int n = mrdegree->reduce(&edge_count,NULL);
    nsingleton = nverts - n;

    // mrhisto KV = (out degree, vert count)
    
    MapReduce *mrhisto = mrdegree;
    mrhisto->clone();
    mrhisto->reduce(&edge_reverse,NULL);
    mrhisto->collate(NULL);
    mrhisto->reduce(&edge_histo,NULL);

    // output sorted histogram of out degree
    // add in inferred zero-degree as last entry

    FILE *fp;
    if (me == 0) {
      fp = fopen(outhfile,"w");
      fprintf(fp,"Out-degree histogram\n");
      fprintf(fp,"Degree vertex-count\n");
    } else fp = NULL;
    
    mrhisto->gather(1);
    mrhisto->sort_keys(&histo_sort);
    mrhisto->clone();
    mrhisto->reduce(&histo_write,fp);

    delete mrhisto;
    
    if (me == 0) {
      fprintf(fp,"%d %d\n",0,nsingleton);
      fclose(fp);
    }
  }

  // compute and output an in-degree histogram

  if (inhfile) {

    // mrdegree = vertices with their out degree as negative value
    // nsingleton_in = # of verts with 0 indegree
    
    if (me == 0) printf("Generating out-degree histogram...\n");
#ifdef NEW_OUT_OF_CORE
    MapReduce *mrdegree = mredge->copy();
#else
    MapReduce *mrdegree = new MapReduce(*mredge);
#endif
    mrdegree->clone();
    mrdegree->reduce(&edge_reverse,NULL);
    mrdegree->collate(NULL);
    int n = mrdegree->reduce(&edge_count,NULL);
    int nsingleton_in = nverts - n;

    // mrhisto KV = (out degree, vert count)
    
    MapReduce *mrhisto = mrdegree;
    mrhisto->clone();
    mrhisto->reduce(&edge_reverse,NULL);
    mrhisto->collate(NULL);
    mrhisto->reduce(&edge_histo,NULL);

    // output sorted histogram of out degree
    // add in inferred zero-degree as last entry

    FILE *fp;
    if (me == 0) {
      fp = fopen(inhfile,"w");
      fprintf(fp,"In-degree histogram\n");
      fprintf(fp,"Degree vertex-count\n");
    } else fp = NULL;
    
    mrhisto->gather(1);
    mrhisto->sort_keys(&histo_sort);
    mrhisto->clone();
    mrhisto->reduce(&histo_write,fp);

    delete mrhisto;
    
    if (me == 0) {
      fprintf(fp,"%d %d\n",0,nsingleton_in);
      fclose(fp);
    }
  }

  // output a Matrix Market file
  // one-line header + one chunk per proc

  if (mfile) {

    if (me == 0) printf("Generating matrix-market file...\n");
    if (me == 0) {
      char fname[128];
      sprintf(fname,"%s.header",mfile);
      FILE *fp = fopen(fname,"w");
      fprintf(fp,"%d %d %d\n",nverts,nverts,nedges);
      fclose(fp);
    }
    
    char fname[128];
    sprintf(fname,"%s.%d",mfile,me);
    FILE *fp = fopen(fname,"w");

    if (mfile_weights) {
#ifdef NEW_OUT_OF_CORE
      MapReduce *mrout = mredge->copy();
#else
      MapReduce *mrout = new MapReduce(*mredge);
#endif
      mrout->convert();
      mrout->reduce(&matrix_write_weights,fp);
      
      delete mrout;
    }
    else {  // mfile_weights == 0
      // print out matrix edges in Matrix Market format with 1/out-degree
      // mrdegree = vertices with their out degree as negative value
      // nsingleton = # of verts with 0 out-degree
    
#ifdef NEW_OUT_OF_CORE
      MapReduce *mrdegree = mredge->copy();
#else
      MapReduce *mrdegree = new MapReduce(*mredge);
#endif
      mrdegree->collate(NULL);
      int n = mrdegree->reduce(&edge_count,NULL);
      nsingleton = nverts - n;

      // mrout KV = (Vi,[Vj Vk ... Vz -outdegree-of-Vi]
      
#ifdef NEW_OUT_OF_CORE
      MapReduce *mrout = mredge->copy();
      mrout->add(mrdegree);
#else
      MapReduce *mrout = new MapReduce(*mredge);
      mrout->kv->add(mrdegree->kv);
#endif
      mrout->collate(NULL);
      mrout->reduce(&matrix_write_inverse_degree,fp);
  
      delete mrdegree;
      delete mrout;
    }

    fclose(fp);
  }

  // clean up

  delete [] outhfile;
  delete [] inhfile;
  delete [] mfile;
  delete [] onefile;

  delete mredge;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0) {
    printf("Graph: %d original edges\n",nrawedges);
    printf("Graph: %d unique vertices\n",nverts);
    printf("Graph: %d unique edges\n",nedges);
    printf("Graph: %d vertices with zero out-degree\n",nsingleton);
    printf("Time:  %g secs\n",tstop-tstart);
  }

  MPI_Finalize();
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
  char buf[CHUNK*RECORDSIZE];

  FILE *fp = fopen(filename,"rb");
  if (fp == NULL) {
    printf("Could not open link file %s\n", filename);
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  while (1) {
    int nrecords = fread(buf,RECORDSIZE,CHUNK,fp);
    char *ptr = buf;
    for (int i = 0; i < nrecords; i++) {
      kv->add(&ptr[0],vertexsize,&ptr[16],vertexsize);
      ptr += RECORDSIZE;
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
  char buf[CHUNK*RECORDSIZE];

  char **files = (char **) ptr;
  FILE *fp = fopen(files[itask],"rb");
  if (fp == NULL) {
    printf("Could not open link file\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  while (1) {
    int nrecords = fread(buf,RECORDSIZE,CHUNK,fp);
    char *ptr = buf;
    for (int i = 0; i < nrecords; i++) {
      kv->add(&ptr[0],vertexsize,&ptr[16],vertexsize);
      ptr += RECORDSIZE;
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
  uint64_t *vi = (uint64_t *) key;
  if (*vi != 0) kv->add((char *) vi,vertexsize,NULL,0);
  uint64_t *vj = (uint64_t *) multivalue;
  if (*vj != 0) kv->add((char *) vj,vertexsize,NULL,0);
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
  uint64_t *vi = (uint64_t *) key;
  if (*vi == 0) return;

  if (vertexsize == 16) {

    map<std::pair<uint64_t, uint64_t>,WEIGHT> hash;

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
      tmp.v[0] = e.first;
      tmp.v[1] = e.second;
      tmp.wt = (*mit).second;
      kv->add(key,keybytes,(char *)&tmp,sizeof(EDGE16));
    }
  } else {  // vertexsize = 8

    map<uint64_t,WEIGHT> hash;

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
      tmp.v[0] = (*mit).first;
      tmp.wt = (*mit).second;
      kv->add(key,keybytes,(char*)&tmp,sizeof(EDGE08));
    }
  }
}

/* ----------------------------------------------------------------------
   vertex_label reduce() function
   input KMV: (Vi,[])
   output KV: (Vi,ID), where ID is a unique int from 1 to N
------------------------------------------------------------------------- */

void vertex_label(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  LABEL *label = (LABEL *) ptr;
  label->count++;
  VERTEX id = label->nthresh + label->count;
  kv->add(key,keybytes,(char *) &id,sizeof(VERTEX));
}

/* ----------------------------------------------------------------------
   edge_label1 reduce() function
   input KMV: (Vi,[{Vj,Wj} {Vk,Wk} ...]), one of the mvalues is a 1-N int ID
   output KV: (Vj,{-IDi,Wj}) (Vk,{-IDi,Wk}) ...
------------------------------------------------------------------------- */

void edge_label1(char *key, int keybytes, char *multivalue,
		 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{


  // Identify id = int ID of vertex key in mvalue list.
  VERTEX id;
  int i, offset;

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] == sizeof(VERTEX)) break;
    offset += valuebytes[i];
  }
  if (i < nvalues) {
    id = - *((VERTEX *) &multivalue[offset]);
    BREAK_BLOCK_LOOP;
  }

  END_BLOCK_LOOP


  // Now relabel vertex key using the ID found and emit reverse edges Vj->key.
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(VERTEX)) {
      // For key, assuming v is first field of both EDGE16 and EDGE8.
      uint64_t *newkey = (uint64_t *)(&multivalue[offset]);
      EDGE val;
      val.v = id;
      if (vertexsize == 16)
        val.wt = (*((EDGE16 *)(&multivalue[offset]))).wt;
      else
        val.wt = (*((EDGE08 *)(&multivalue[offset]))).wt;
      kv->add((char*)newkey,vertexsize,(char*)&val,sizeof(EDGE));
    }
    offset += valuebytes[i];
  }

  END_BLOCK_LOOP
}

/* ----------------------------------------------------------------------
   edge_label2 reduce() function
   input KMV: (Vi,[{-IDj,Wi} {-IDk,Wi} ...]+one of the mvalues is a positive 
   int = IDi.
   Note that the edges are backward on input.  
   And Wi can differ for each IDj.
   output KV: (IDj,{IDi,Wi}) (IDk,{IDi,Wi}) ...
------------------------------------------------------------------------- */

void edge_label2(char *key, int keybytes, char *multivalue,
		 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{

  // id = positive int in mvalue list

  int i;
  int offset;
  VERTEX id;

  // Identify id = int ID of vertex key in mvalue list.
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] == sizeof(VERTEX)) break;
    offset += valuebytes[i];
  }
  if (i < nvalues) {
    id = *((VERTEX *) &multivalue[offset]);
    BREAK_BLOCK_LOOP;
  }

  END_BLOCK_LOOP

  // Now relabel vertex key using the ID found and emit edges key->Vj using IDs.
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(VERTEX)) {
      EDGE *mv = (EDGE *)&(multivalue[offset]);
      VERTEX vi = -(mv->v);
      EDGE tmp;
      tmp.v = id;
      tmp.wt = mv->wt;
      kv->add((char *) &vi,sizeof(VERTEX),(char *) &tmp,sizeof(EDGE));
    }
    offset += valuebytes[i];
  }

  END_BLOCK_LOOP
}

/* ----------------------------------------------------------------------
   edge_count reduce() function
   input KMV: (Vi,[Vj Vk ...])
   output KV: (Vi,degree), degree as negative value
------------------------------------------------------------------------- */

void edge_count(char *key, int keybytes, char *multivalue,
		int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int value = -nvalues;
  kv->add(key,keybytes,(char *) &value,sizeof(int));
}

/* ----------------------------------------------------------------------
   edge_reverse reduce() function
   input KMV: (Vi,[value])
   output KV: (value,Vi)
------------------------------------------------------------------------- */

void edge_reverse(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int value = - *((int *) multivalue);
  kv->add((char *) &value,sizeof(int),key,keybytes);
}

/* ----------------------------------------------------------------------
   edge_histo reduce() function
   input KMV: (degree,[Vi Vj ...])
   output KV: (degree,ncount) where ncount = # of V in multivalue
------------------------------------------------------------------------- */

void edge_histo(char *key, int keybytes, char *multivalue,
		int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,(char *) &nvalues,sizeof(int));
}

/* ----------------------------------------------------------------------
   histo_sort compare() function
   sort degree values in reverse order
------------------------------------------------------------------------- */

int histo_sort(char *value1, int len1, char *value2, int len2)
{
  int *i1 = (int *) value1;
  int *i2 = (int *) value2;
  if (*i1 > *i2) return -1;
  else if (*i1 < *i2) return 1;
  else return 0;
}

/* ----------------------------------------------------------------------
   histo_write reduce() function
   input KMV: (degree,count)
   write pair to file, create no new KV
------------------------------------------------------------------------- */

void histo_write(char *key, int keybytes, char *multivalue,
		int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  FILE *fp = (FILE *) ptr;

  int *degree = (int *) key;
  int *count = (int *) multivalue;
  fprintf(fp,"%d %d\n",*degree,*count);
}

/* ----------------------------------------------------------------------
   hfile_write reduce() function
   input KMV: (Vi,[{Vj,Wj} {Vk Wj} ...])
   write each edge to file, create no new KV
------------------------------------------------------------------------- */

void hfile_write(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int i;
  FILE *fp = (FILE *) ptr;

  uint64_t *vi = (uint64_t *) key;


  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  if (keybytes == 16) {
    // Two 64-bit ints per vertex
    EDGE16 *edge = (EDGE16 *) multivalue;
    for (i = 0; i < nvalues; i++)
      fprintf(fp,"%llu %llu    %llu %llu\n",
                  vi[0], vi[1] ,edge[i].v[0], edge[i].v[1]);
  }
  else if (keybytes == 8) {
    // One 64-bit int per vertex
    EDGE08 *edge = (EDGE08 *) multivalue;
    for (i = 0; i < nvalues; i++)
      fprintf(fp,"%llu   %llu\n", *vi, edge[i].v[0]);
  }
  else 
    fprintf(fp, "Invalid vertex size %d\n", keybytes);

  END_BLOCK_LOOP
}

/* ----------------------------------------------------------------------
   matrix_write_inverse_degree reduce() function
   input KMV: (Vi,[{Vj Wj} {Vk Wk} ...]), one of the mvalues is a negative int
   negative int is degree of Vi
   write each edge to file, create no new KV
------------------------------------------------------------------------- */

void matrix_write_inverse_degree(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int i;

  FILE *fp = (FILE *) ptr;

  int offset;
  double inverse_outdegree;
  VERTEX vi = *((VERTEX *) key);

  // First, find the negative int, which is -degree of Vi.
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] == sizeof(int)) break;
    offset += valuebytes[i];
  }
  if (i < nvalues) {
    int tmp = *((int *) &multivalue[offset]);
    inverse_outdegree = -1.0/tmp;
    BREAK_BLOCK_LOOP;
  }

  END_BLOCK_LOOP


  // Then, output the edges Vi->Vj with value 1/degree_of_Vi
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(int)) {
      EDGE *edge = (EDGE *) &multivalue[offset];
      fprintf(fp,"%d %d %g\n",vi,edge->v,inverse_outdegree);
    }
    offset += valuebytes[i];
  }

  END_BLOCK_LOOP
}

/* ----------------------------------------------------------------------
   matrix_write_weights reduce() function
   input KMV: (Vi,[{Vj Wj} {Vk Wk} ...]), 
   where Wj is the weight of edge Vi->Vj.
   write each edge to file, create no new KV
------------------------------------------------------------------------- */
void matrix_write_weights(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int i;
  FILE *fp = (FILE *) ptr;
  VERTEX vi = *((VERTEX *) key);

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  EDGE *edge = (EDGE *) multivalue;

  for (i = 0; i < nvalues; i++)
    fprintf(fp,"%d %d %d.\n",vi,edge[i].v,edge[i].wt);

  END_BLOCK_LOOP
}
