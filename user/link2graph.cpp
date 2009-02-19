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
  int convertflag = 0;
  char *onefile = NULL;
  int nfiles = 0;
  char **argfiles = NULL;

  int flag = 0;
  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-out") == 0) {
      if (iarg+2 > narg) {
	flag = 1;
	break;
      }
      int n = strlen(args[iarg+1]) + 1;
      outhfile = new char[n];
      strcpy(outhfile,args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-in") == 0) {
      if (iarg+2 > narg) {
	flag = 1;
	break;
      }
      int n = strlen(args[iarg+1]) + 1;
      inhfile = new char[n];
      strcpy(inhfile,args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-m") == 0) {
      if (iarg+2 > narg) {
	flag = 1;
	break;
      }
      int n = strlen(args[iarg+1]) + 1;
      mfile = new char[n];
      strcpy(mfile,args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-c") == 0) {
      if (iarg+1 > narg) {
	flag = 1;
	break;
      }
      convertflag = 1;
      iarg += 1;
    } else if (strcmp(args[iarg],"-e1") == 0) {
      if (iarg+1 > narg) {
	flag = 1;
	break;
      }
      vertexsize = 8;
      iarg += 1;
    } else if (strcmp(args[iarg],"-e2") == 0) {
      if (iarg+1 > narg) {
	flag = 1;
	break;
      }
      vertexsize = 16;
      iarg += 1;
    } else if (strcmp(args[iarg],"-ff") == 0) {
      if (iarg+2 > narg) {
	flag = 1;
	break;
      }
      int n = strlen(args[iarg+1]) + 1;
      onefile = new char[n];
      strcpy(onefile,args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-f") == 0) {
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
  int nrawedges;
  if (onefile) nrawedges = mrraw->map(onefile,&fileread1,NULL);
  else nrawedges = mrraw->map(nfiles,&fileread2,argfiles);

  // mrvert = unique non-zero vertices

  MapReduce *mrvert = new MapReduce(*mrraw);
  mrvert->clone();
  mrvert->reduce(&vertex_emit,NULL);
  mrvert->collate(NULL);
  int nverts = mrvert->reduce(&vertex_unique,NULL);

  // mredge = unique I->J edges with I and J non-zero
  // no longer need mrraw

  MapReduce *mredge = new MapReduce(*mrraw);
  mredge->collate(NULL);
  int nedges = mredge->reduce(&edge_unique,NULL);
  delete mrraw;

  // set nsingleton to -1 in case never compute it via options

  int nsingleton = -1;

  // update mredge so its vertices are unique ints from 1-N, not hash values

  if (convertflag) {

    // mrvertlabel = vertices with unique IDs 1-N
    // label.nthresh = # of verts on procs < me
    // no longer need mrvert

    LABEL label;
    label.count = 0;
    int nlocal = mrvert->kv->nkey;
    MPI_Scan(&nlocal,&label.nthresh,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
    label.nthresh -= nlocal;

    MapReduce *mrvertlabel = new MapReduce(*mrvert);
    mrvertlabel->clone();
    mrvertlabel->reduce(&vertex_label,&label);
    
    delete mrvert;

    // reset all vertices in mredge from 1 to N

    mredge->kv->add(mrvertlabel->kv);
    mredge->collate(NULL);
    mredge->reduce(&edge_label1,NULL);
    mredge->kv->add(mrvertlabel->kv);
    mredge->collate(NULL);
    mredge->reduce(&edge_label2,NULL);
    
    delete mrvertlabel;
  }

  // compute and output an out-degree histogram

  if (outhfile) {

    // mrdegree = vertices with their out degree as negative value
    // nsingleton = # of verts with 0 degree
    
    MapReduce *mrdegree = new MapReduce(*mredge);
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
    // nsingleton_in = # of verts with 0 degree
    
    MapReduce *mrdegree = new MapReduce(*mredge);
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
    // nsingleton = # of verts with 0 degree
    
    MapReduce *mrdegree = new MapReduce(*mredge);
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
    
    MapReduce *mrout = new MapReduce(*mredge);
    mrout->kv->add(mrdegree->kv);
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
    printf("Graph: %d singleton vertices\n",nsingleton);
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

    int offset = 0;
    for (int i = 0; i < nvalues; i++) {
      unsigned long *v1 = (unsigned long *) &multivalue[offset];
      unsigned long *v2 = (unsigned long *) &multivalue[offset+8];
      if (hash.find(std::make_pair(*v1,*v2)) == hash.end())
	hash[std::make_pair(*v1,*v2)] = 0;
      else continue;
      kv->add(key,keybytes,&multivalue[offset],vertexsize);
      offset += valuebytes[i];
    }

  } else {
    std::map<unsigned long,int> hash;

    unsigned long *vertex = (unsigned long *) multivalue;
    for (int i = 0; i < nvalues; i++) {
      if (vertex[i] == 0) continue;
      if (hash.find(vertex[i]) == hash.end()) hash[vertex[i]] = 0;
      else continue;
      kv->add(key,keybytes,(char *) &vertex[i],vertexsize);
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
   output KV: (Vj,degree), degree as negative value
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
