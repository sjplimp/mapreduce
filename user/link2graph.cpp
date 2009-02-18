// convert Karl's link files into a graph
// print stats on resulting graph
// optionally output graph as Matrix Market file

// Syntax: link2graph file1 file2 ...

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"

#include <map>

using namespace MAPREDUCE_NS;

void fileread(int, KeyValue *, void *);
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

#define VERTEXSIZE 8
#define RECORDSIZE 32
#define CHUNK 8192

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  if (narg <= 1) {
    if (me == 0) printf("Syntax: link2graph file1 file2 ...\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // mrraw = all edges in file data

  MapReduce *mrraw = new MapReduce(MPI_COMM_WORLD);
  mrraw->verbosity = 1;
  int nrawedges = mrraw->map(narg-1,&fileread,&args[1]);

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
  int count = 0;
  mrvertlabel->reduce(&vertex_label,&label);

  delete mrvert;

  // reset all vertices in mredge from 1 to N

  mredge->kv->add(mrvertlabel->kv);
  mredge->collate(NULL);
  mredge->reduce(&edge_label1,NULL);
  mredge->kv->add(mrvertlabel->kv);
  mredge->collate(NULL);
  mredge->reduce(&edge_label2,NULL);

  // mrdegree = vertices with their out degree
  // nvertdegree = # of verts with non-zero degree

  MapReduce *mrdegree = new MapReduce(*mredge);
  mrdegree->collate(NULL);
  int n = mrdegree->reduce(&edge_count,NULL);
  int nsingleton = nverts - n;

  // mrhisto KV = (out degree, vert count)

  mrdegree->clone();
  mrdegree->reduce(&edge_reverse,NULL);
  mrdegree->collate(NULL);
  mrdegree->reduce(&edge_histo,NULL);

  // output sorted histogram of out degree
  // add in inferred zero-degree as last entry

  FILE *fp;
  if (me == 0) {
    fp = fopen("a.histo","w");
    fprintf(fp,"Outdegree histogram\n");
    fprintf(fp,"Degree vertex-count\n");
  } else fp = NULL;
      
  mrdegree->gather(1);
  mrdegree->sort_keys(&histo_sort);
  mrdegree->clone();
  mrdegree->reduce(&histo_write,fp);
  delete mrdegree;

  if (me == 0) {
    fprintf(fp,"%d %d\n",0,nsingleton);
    fclose(fp);
  }

  // print out matrix edges in Matrix Market format
  // this destroys mredge

  if (me == 0) {
    fp = fopen("a.header","w");
    fprintf(fp,"%d %d %d\n",nverts,nverts,nedges);
    fclose(fp);
  }

  char fname[16];
  sprintf(fname,"a.%d",me);
  fp = fopen(fname,"w");
  mredge->clone();
  mredge->reduce(&matrix_write,fp);
  fclose(fp);

  // clean up

  delete mrvertlabel;
  delete mredge;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0) {
    printf("Graph: %d original edges\n",nrawedges);
    printf("Graph: %d unique vertices\n",nverts);
    printf("Graph: %d unique edges\n",nedges);
    printf("Graph: %d singleton vertices\n",nsingleton);
  }

  MPI_Finalize();
}

/* ----------------------------------------------------------------------
   fileread map() function
   for each record in file, Vi = "from host", Vj = "to host"
   output KV: (Vi,Vj)
------------------------------------------------------------------------- */

void fileread(int itask, KeyValue *kv, void *ptr)
{
  char buf[CHUNK*RECORDSIZE];

  char **files = (char **) ptr;
  FILE *fp = fopen(files[itask],"rb");

  while (1) {
    int nrecords = fread(buf,RECORDSIZE,CHUNK,fp);
    char *ptr = buf;
    for (int i = 0; i < nrecords; i++) {
      kv->add(&ptr[0],8,&ptr[16],8);
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
   omit any Vi = 0
------------------------------------------------------------------------- */

void vertex_emit(char *key, int keybytes, char *multivalue,
		 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  unsigned long *vi = (unsigned long *) key;
  if (*vi != 0) kv->add((char *) vi,VERTEXSIZE,NULL,0);
  unsigned long *vj = (unsigned long *) multivalue;
  if (*vj != 0) kv->add((char *) vj,VERTEXSIZE,NULL,0);
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
   only non-zero Vi or Vj are emitted
   only unique edges are emitted
------------------------------------------------------------------------- */

void edge_unique(char *key, int keybytes, char *multivalue,
		 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  unsigned long *vi = (unsigned long *) key;
  if (*vi == 0) return;

  std::map<unsigned long,int> hash;

  unsigned long *vertex = (unsigned long *) multivalue;
  for (int i = 0; i < nvalues; i++) {
    if (vertex[i] == 0) continue;
    if (hash.find(vertex[i]) == hash.end()) hash[vertex[i]] = 0;
    else continue;
    kv->add(key,keybytes,(char *) &vertex[i],VERTEXSIZE);
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
    if (valuebytes[i] == VERTEXSIZE)
      kv->add(&multivalue[offset],VERTEXSIZE,(char *) &id,sizeof(int));
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
   output KV: (Vj,degree)
------------------------------------------------------------------------- */

void edge_count(char *key, int keybytes, char *multivalue,
		int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,(char *) &nvalues,sizeof(int));
}

/* ----------------------------------------------------------------------
   edge_reverse reduce() function
   input KMV: (Vi,[degree])
   output KV: (degree,Vi)
------------------------------------------------------------------------- */

void edge_reverse(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(multivalue,sizeof(int),key,keybytes);
}

/* ----------------------------------------------------------------------
   edge_histo reduce() function
   input KMV: (degree,[Vi Vj ...])
   output KV: (degree,ncount)
------------------------------------------------------------------------- */

void edge_histo(char *key, int keybytes, char *multivalue,
		int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,(char *) &nvalues,sizeof(int));
}

/* ----------------------------------------------------------------------
   edge_histo reduce() function
   input KMV: (degree,[Vi Vj ...])
   output KV: (degree,ncount)
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
   edge_histo reduce() function
   input KMV: (degree,[Vi Vj ...])
   output KV: (degree,ncount)
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
   input KMV: (Vi,[Vj])
   write edge to file, create no new KV
------------------------------------------------------------------------- */

void matrix_write(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  FILE *fp = (FILE *) ptr;

  //unsigned long *vi = ((unsigned long *) key);
  //unsigned long *vj = ((unsigned long *) multivalue);
  //fprintf(fp,"%lu %lu 0\n",*vi,*vj);

  int vi = *((int *) key);
  int vj = *((int *) multivalue);
  fprintf(fp,"%d %d 0\n",vi,vj);
}
