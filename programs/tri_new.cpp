// Jon Cohen algorithm to enumerate triangles in a graph
// Syntax: try.py infile outfile
//   infile = matrix market file of edges
//   outfile = list of triangles, 3 vertices per line

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.hpp"

using namespace MAPREDUCE_NS;

void fileread(int, char *, int, KeyValue *, void *);
void invert_edges(int, char *, int, char *, int, KeyValue *, void *);
void remove_duplicates(char *, int, char *, int, int *, KeyValue *, void *);
void emit_vertices(int, char *, int, char *, int, KeyValue *, void *);
void first_degree(char *, int, char *, int, int *, KeyValue *, void *);
void second_degree(char *, int, char *, int, int *, KeyValue *, void *);
void low_degree(int, char *, int, char *, int, KeyValue *, void *);
void nsq_angles(char *, int, char *, int, int *, KeyValue *, void *);
void emit_triangles(char *, int, char *, int, int *, KeyValue *, void *);
void output_triangle(int, char *, int, char *, int, KeyValue *, void *);

typedef int VERTEX;      // vertex ID

typedef struct {         // edge = 2 vertices
  VERTEX vi,vj;
} EDGE;

typedef struct {         // tri = 3 vertices
  VERTEX vi,vj,vk;
} TRI;

typedef struct {         // degree of 2 edge vertices
  int di,dj;
} DEGREE;


/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  // parse command-line args

  if (narg != 3) {
    if (me == 0) printf("Syntax: tri infile outfile\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  char *infile,*outfile;
  int n = strlen(args[1]) + 1;
  infile = new char[n];
  strcpy(infile,args[1]);
  n = strlen(args[2]) + 1;
  outfile = new char[n];
  strcpy(outfile,args[2]);

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->set_fpath(MYLOCALDISK);
  mr->verbosity = 1;
  mr->timer = 1;
  //mr->memsize = 1;
  mr->memsize = 128;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // read input file
  // results in ((vi,vj),None)

  int nedges = mr->map(100*nprocs,1,&infile,'\n',80,&fileread,NULL);
  //int nedges = mr->map(nprocs,1,&infile,'\n',80,&fileread,NULL);
  if (me == 0) printf("%d edges in input file\n",nedges);

  // eliminate duplicate edges = both I,J and J,I exist
  // results in ((vi,vj),None) with all vi < vj

  mr->map(mr,&invert_edges,NULL);
  mr->collate(NULL);
  nedges = mr->reduce(&remove_duplicates,NULL);
  if (me == 0) printf("%d edges after duplicates removed\n",nedges);

  // make copy of graph for use in triangle finding

  MapReduce *mrcopy = mr->copy();

  // augment edges with degree of each vertex
  // results in ((vi,vj),(deg(vi),deg(vj)) with all vi < vj

  mr->map(mr,&emit_vertices,NULL);
  mr->collate(NULL);
  mr->reduce(&first_degree,NULL);
  mr->collate(NULL);
  mr->reduce(&second_degree,NULL);
  if (me == 0) printf("%d edges augmented by vertex degrees\n",nedges);

  // find triangles in degree-augmented graph and write to output file
  // once create angles, add in edges from original graph
  // this enables finding completed triangles in emit_triangles()
  // results in ((vi,vj,vk),None)

  mr->map(mr,&low_degree,NULL);
  mr->collate(NULL);
  mr->reduce(&nsq_angles,NULL);
  mr->add(mrcopy);
  mr->collate(NULL);
  int ntri = mr->reduce(&emit_triangles,NULL);
  if (me == 0) printf("%d triangles\n",ntri);

  char fname[128];
  sprintf(fname,"%s.%d",outfile,me);
  FILE *fp = fopen(fname,"w");
  if (fp == NULL) {
    printf("ERROR: Could not open output file");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
  mr->map(mr,&output_triangle,fp);
  fclose(fp);

  // timing data

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0)
    printf("%g secs to find triangles on %d procs\n",tstop-tstart,nprocs);

  // clean up

  delete mrcopy;
  delete mr;
  delete [] infile;
  delete [] outfile;
  MPI_Finalize();
}

// ----------------------------------------------------------------------
// maps and reduces

// read portion of input file
// all procs trim last line(s) (between newline and NULL)
// 1st proc trims header line(s)
// emit one KV per edge: key = (vi,vj), value = None

void fileread(int itask, char *bytes, int nbytes, KeyValue *kv, void *ptr)
{
  EDGE edge;

  char *line = strtok(bytes,"\n");
  if (itask == 0) {
    while (line[0] == '%') line = strtok(NULL,"\n");
    line = strtok(NULL,"\n");
  }

  while (line) {
    if (strlen(line)) {
      sscanf(line,"%d %d",&edge.vi,&edge.vj);
      kv->add((char *) &edge,sizeof(EDGE),NULL,0);
    }
    line = strtok(NULL,"\n");
  }
}

// invert edges so all have vi < vj
// drop edges where vi = vj

void invert_edges(int itask, char *key, int keybytes, char *value,
		  int valuebytes, KeyValue *kv, void *ptr) 
{
  EDGE *edge = (EDGE *) key;
  if (edge->vi < edge->vj) kv->add((char *) edge,sizeof(EDGE),NULL,0);
  else if (edge->vj < edge->vi) {
    EDGE edgeflip;
    edgeflip.vi = edge->vj;
    edgeflip.vj = edge->vi;
    kv->add((char *) &edgeflip,sizeof(EDGE),NULL,0);
  }
}

// remove duplicate edges
// if I,J and J,I were in graph, now just one copy with I < J will be

void remove_duplicates(char *key, int keybytes, char *multivalue,
		       int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,NULL,0);
}

// convert edge key to vertex keys
// emit two KV per edge: (vi,vj) and (vj,vi)

void emit_vertices(int itask, char *key, int keybytes, char *value,
		   int valuebytes, KeyValue *kv, void *ptr) 
{
  EDGE *edge = (EDGE *) key;
  kv->add((char *) &edge->vi,sizeof(VERTEX),(char *) &edge->vj,sizeof(VERTEX));
  kv->add((char *) &edge->vj,sizeof(VERTEX),(char *) &edge->vi,sizeof(VERTEX));
}

// assign degree of 1st vertex
// emit one KV per edge: ((vi,vj),(degree,0)) or ((vi,vj),(0,degree))
// where vi < vj and degree is assigned to correct vertex

void first_degree(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  VERTEX vi,vj;
  EDGE edge;
  DEGREE degree;

  vi = *((VERTEX *) key);
  int offset = 0;
  for (int i = 0; i < nvalues; i++) {
    vj = *((VERTEX *) &multivalue[offset]);
    offset += valuebytes[i];
    if (vi < vj) {
      edge.vi = vi;
      edge.vj = vj;
      degree.di = nvalues;
      degree.dj = 0;
      kv->add((char *) &edge,sizeof(EDGE),(char *) &degree,sizeof(DEGREE));
    } else {
      edge.vi = vj;
      edge.vj = vi;
      degree.di = 0;
      degree.dj = nvalues;
      kv->add((char *) &edge,sizeof(EDGE),(char *) &degree,sizeof(DEGREE));
    }
  }
}

// assign degree of 2nd vertex
// 2 values per edge, with degree of vi and vj
// emit one KV per edge: ((vi,vj),(deg(i),deg(j))) with vi < vj

void second_degree(char *key, int keybytes, char *multivalue,
		   int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  DEGREE *one = (DEGREE *) multivalue;
  DEGREE *two = (DEGREE *) &multivalue[valuebytes[0]];
  DEGREE degree;

  if (one->di) {
    degree.di = one->di;
    degree.dj = two->dj;
    kv->add(key,keybytes,(char *) &degree,sizeof(DEGREE));
  } else {
    degree.di = two->di;
    degree.dj = one->dj;
    kv->add(key,keybytes,(char *) &degree,sizeof(DEGREE));
  }
}

// low-degree vertex emits (vi,vj)
// break tie with low-index vertex

void low_degree(int itask, char *key, int keybytes, char *value,
		int valuebytes, KeyValue *kv, void *ptr) 
{
  EDGE *edge = (EDGE *) key;
  DEGREE *degree = (DEGREE *) value;

  if (degree->di < degree->dj)
    kv->add((char *) &edge->vi,sizeof(VERTEX),
	    (char *) &edge->vj,sizeof(VERTEX));
  else if (degree->dj < degree->di)
    kv->add((char *) &edge->vj,sizeof(VERTEX),
	    (char *) &edge->vi,sizeof(VERTEX));
  else if (edge->vi < edge->vj)
    kv->add((char *) &edge->vi,sizeof(VERTEX),
	    (char *) &edge->vj,sizeof(VERTEX));
  else
    kv->add((char *) &edge->vj,sizeof(VERTEX),
	    (char *) &edge->vi,sizeof(VERTEX));
}

// emit Nsq angles associated with each central vertex vi
// emit KV as ((vj,vk),vi) where vj < vk

void nsq_angles(char *key, int keybytes, char *multivalue,
		int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  VERTEX vj,vk;
  EDGE edge;

  for (int j = 0; j < nvalues-1; j++) {
    vj = *((VERTEX *) &multivalue[j*sizeof(VERTEX)]);
    for (int k = j+1; k < nvalues; k++) {
      vk = *((VERTEX *) &multivalue[k*sizeof(VERTEX)]);
      if (vj < vk) {
	edge.vi = vj;
	edge.vj = vk;
	kv->add((char *) &edge,sizeof(EDGE),key,sizeof(VERTEX));
      } else {
	edge.vi = vk;
	edge.vj = vj;
	kv->add((char *) &edge,sizeof(EDGE),key,sizeof(VERTEX));
      }
    }
  }
}

// if NULL exists in mvalue, emit other values as triangles
// emit KV as ((vi,vj,vk),None)

void emit_triangles(char *key, int keybytes, char *multivalue,
		    int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int i;
  for (i = 0; i < nvalues; i++)
    if (valuebytes[i] == 0) break;
  if (i == nvalues) return;

  TRI tri;
  EDGE *edge = (EDGE *) key;
  tri.vj = edge->vi;
  tri.vk = edge->vj;

  int offset = 0;
  for (int i = 0; i < nvalues; i++)
    if (valuebytes[i]) {
      tri.vi = *((VERTEX *) &multivalue[offset]);
      kv->add((char *) &tri,sizeof(TRI),NULL,0);
      offset += valuebytes[i];
    }
}

// print triangles to local file

void output_triangle(int itask, char *key, int keybytes, char *value,
		     int valuebytes, KeyValue *kv, void *ptr)
{
  FILE *fp = (FILE *) ptr;
  TRI *tri = (TRI *) key;
  fprintf(fp,"%d %d %d\n",tri->vi,tri->vj,tri->vk);
}
