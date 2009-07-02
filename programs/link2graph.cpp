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
void matrix_write(char *, int, char *, int, int *, KeyValue *, void *);

typedef struct {
  int nthresh;
  int count;
} LABEL;

#define RECORDSIZE 32
#define CHUNK 8192

static int vertexsize = 8;

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

  int nrawedges;
  if (onefile) nrawedges = mrraw->map(onefile,&fileread1,NULL);
  else nrawedges = mrraw->map(nfiles,&fileread2,argfiles);

  // mrvert = unique non-zero vertices

#ifdef NEW_OUT_OF_CORE
  MapReduce *mrvert = mrraw->copy();
#else
  MapReduce *mrvert = new MapReduce(*mrraw);
#endif

  mrvert->clone();
  mrvert->reduce(&vertex_emit,NULL);
  mrvert->collate(NULL);
  int nverts = mrvert->reduce(&vertex_unique,NULL);

  // mredge = unique I->J edges with I and J non-zero
  // no longer need mrraw

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

    mrout->collate(NULL);
    mrout->reduce(&hfile_write,fp);

    delete mrout;

    fclose(fp);
  }

  // update mredge so its vertices are unique ints from 1-N, not hash values

  if (convertflag) {

    // mrvertlabel = vertices with unique IDs 1-N
    // label.nthresh = # of verts on procs < me
    // no longer need mrvert

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

    // mrout KV = (Vi,[Vj Vk ... Vz outdegree-of-Vi]
    // print out matrix edges in Matrix Market format with 1/out-degree
    
#ifdef NEW_OUT_OF_CORE
    MapReduce *mrout = mredge->copy();
    mrout->add(mrdegree);
#else
    MapReduce *mrout = new MapReduce(*mredge);
    mrout->kv->add(mrdegree->kv);
#endif
    mrout->collate(NULL);
    mrout->reduce(&matrix_write,fp);

    delete mrdegree;
    delete mrout;

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
  unsigned long *vi = (unsigned long *) key;
  if (*vi != 0) kv->add((char *) vi,vertexsize,NULL,0);
  unsigned long *vj = (unsigned long *) multivalue;
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
   output KV: (Vi,Vj) (Vi,Vk) ...
   only an edge where first 8 bytes of Vi or Vj are both non-zero is emitted
   only unique edges are emitted
------------------------------------------------------------------------- */

void edge_unique(char *key, int keybytes, char *multivalue,
		 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  unsigned long *vi = (unsigned long *) key;
  if (*vi == 0) return;

  if (vertexsize == 16) {
    std::map<std::pair<unsigned long, unsigned long>,int> hash;

#ifdef NEW_OUT_OF_CORE
    if (multivalue) {
#endif
      int offset = 0;
      for (int i = 0; i < nvalues; i++) {
        unsigned long *v1 = (unsigned long *) &multivalue[offset];
        unsigned long *v2 = (unsigned long *) &multivalue[offset+8];
        if (hash.find(std::make_pair(*v1,*v2)) == hash.end())
          hash[std::make_pair(*v1,*v2)] = 0;
        else {
          offset += valuebytes[i];
          continue;
        }
        kv->add(key,keybytes,&multivalue[offset],vertexsize);
        offset += valuebytes[i];
      }
#ifdef NEW_OUT_OF_CORE
    } else {
      // Multivalue is in multiple blocks; retrieve blocks one-by-one.
      MapReduce *mr = (MapReduce *) valuebytes;
      int nblocks = mr->multivalue_blocks();
      for (int iblock = 0; iblock < nblocks; iblock++) {
        nvalues = mr->multivalue_block(iblock,&multivalue,&valuebytes);
        int offset = 0;
        for (int i = 0; i < nvalues; i++) {
          unsigned long *v1 = (unsigned long *) &multivalue[offset];
          unsigned long *v2 = (unsigned long *) &multivalue[offset+8];
          if (hash.find(std::make_pair(*v1,*v2)) == hash.end())
            hash[std::make_pair(*v1,*v2)] = 0;
          else {
            offset += valuebytes[i];
            continue;
          }
          kv->add(key,keybytes,&multivalue[offset],vertexsize);
          offset += valuebytes[i];
        }
      }
    }
#endif
  } else {
    std::map<unsigned long,int> hash;

#ifdef NEW_OUT_OF_CORE
    if (multivalue) {
#endif
      unsigned long *vertex = (unsigned long *) multivalue;
      for (int i = 0; i < nvalues; i++) {
        if (vertex[i] == 0) continue;
        if (hash.find(vertex[i]) == hash.end()) hash[vertex[i]] = 0;
        else continue;
        kv->add(key,keybytes,(char *) &vertex[i],vertexsize);
      }
#ifdef NEW_OUT_OF_CORE
    } else {
      // Multivalue is in multiple blocks; retrieve blocks one-by-one.
      MapReduce *mr = (MapReduce *) valuebytes;
      int nblocks = mr->multivalue_blocks();
      for (int iblock = 0; iblock < nblocks; iblock++) {
        nvalues = mr->multivalue_block(iblock,&multivalue,&valuebytes);
        unsigned long *vertex = (unsigned long *) multivalue;
        for (int i = 0; i < nvalues; i++) {
          if (vertex[i] == 0) continue;
          if (hash.find(vertex[i]) == hash.end()) hash[vertex[i]] = 0;
          else continue;
          kv->add(key,keybytes,(char *) &vertex[i],vertexsize);
        }
      }
    }
#endif
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
  int id = label->nthresh + label->count;
  kv->add(key,keybytes,(char *) &id,sizeof(int));
}

/* ----------------------------------------------------------------------
   edge_label1 reduce() function
   input KMV: (Vi,[Vj Vk ...]), one of the mvalues is a 1-N int ID
   output KV: (Vj,ID) (Vj,ID) ...
------------------------------------------------------------------------- */

void edge_label1(char *key, int keybytes, char *multivalue,
		 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  // id = int ID in mvalue list

  int offset = 0;
  for (int i = 0; i < nvalues; i++) {
    if (valuebytes[i] == sizeof(int)) break;
    offset += valuebytes[i];
  }
  int id = - *((int *) &multivalue[offset]);

  offset = 0;
  for (int i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(int))
      kv->add(&multivalue[offset],valuebytes[i],(char *) &id,sizeof(int));
    offset += valuebytes[i];
  }
}

/* ----------------------------------------------------------------------
   edge_label2 reduce() function
   input KMV: (Vi,[Vj Vk ...]), one of the mvalues is a positive int = ID
   output KV: (Vj,ID) (Vj,ID) ...
------------------------------------------------------------------------- */

void edge_label2(char *key, int keybytes, char *multivalue,
		 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int i,vi;

  // id = negative int in mvalue list

  int *vertex = (int *) multivalue;
  for (i = 0; i < nvalues; i++)
    if (vertex[i] > 0) break;
  int id = vertex[i];

  for (i = 0; i < nvalues; i++) {
    if (vertex[i] < 0) {
      vi = -vertex[i];
      kv->add((char *) &vi,sizeof(int),(char *) &id,sizeof(int));
    }
  }
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
   input KMV: (Vi,[Vj Vk ...])
   write each edge to file, create no new KV
------------------------------------------------------------------------- */

void hfile_write(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int i;
  FILE *fp = (FILE *) ptr;

  uint64_t *vertex = (uint64_t *) multivalue;
  uint64_t *vi = (uint64_t *) key;

  if (keybytes == 16) {
    // Two 64-bit ints per vertex
    for (i = 0; i < nvalues; i++)
      fprintf(fp,"%lld %lld    %lld %lld\n",
                  vi[0], vi[1] ,vertex[i*2], vertex[i*2+1]);
  }
  else if (keybytes == 8) {
    // One 64-bit int per vertex
    for (i = 0; i < nvalues; i++)
      fprintf(fp,"%lld   %lld\n", *vi, vertex[i]);
  }
  else {
    fprintf(fp, "Invalid vertex size %d\n", keybytes);
  }
}

/* ----------------------------------------------------------------------
   matrix_write reduce() function
   input KMV: (Vi,[Vj Vk ...]), one of the mvalues is a negative int
   negative int is degree of Vi
   write each edge to file, create no new KV
------------------------------------------------------------------------- */

void matrix_write(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int i;

  FILE *fp = (FILE *) ptr;

  int *vertex = (int *) multivalue;
  for (i = 0; i < nvalues; i++)
    if (vertex[i] < 0) break;
  double inverse_outdegree = -1.0/vertex[i];

  int vi = *((int *) key);

  for (i = 0; i < nvalues; i++)
    if (vertex[i] > 0) 
      fprintf(fp,"%d %d %g\n",vi,vertex[i],inverse_outdegree);
}
