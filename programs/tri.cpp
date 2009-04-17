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

using namespace MAPREDUCE_NS;

void fileread(int, char *, int, KeyValue *, void *);
void invert_edges(char *, int, char *, int, int *, KeyValue *, void *);
void remove_duplicates(char *, int, char *, int, int *, KeyValue *, void *);
void emit_vertices(char *, int, char *, int, int *, KeyValue *, void *);
void first_degree(char *, int, char *, int, int *, KeyValue *, void *);
void second_degree(char *, int, char *, int, int *, KeyValue *, void *);
void low_degree(char *, int, char *, int, int *, KeyValue *, void *);
void nsq_angles(char *, int, char *, int, int *, KeyValue *, void *);
void emit_triangles(char *, int, char *, int, int *, KeyValue *, void *);
void output_triangle(char *, int, char *, int, int *, KeyValue *, void *);

typedef int VERTEX;      // vertex ID

typedef struct {         // edge = 2 vertices
  VERTEX vi,vj;
} EDGE;

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
  n = strlen(args[1]) + 1;
  outfile = new char[n];
  strcpy(outfile,args[1]);

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // read input file
  // results in ((vi,vj),None)

  int nedges = mr->map(nprocs,1,&infile,'\n',80,&fileread,NULL);
  if (me == 0) printf("%d edges in input file\n",nedges);

  // eliminate duplicate edges = both I,J and J,I exist
  // results in ((vi,vj),None) with all vi < vj

  mr->clone();
  mr->reduce(&invert_edges,NULL);
  mr->collate(NULL);
  nedges = mr->reduce(&remove_duplicates,NULL);
  if (me == 0) printf("%d edges after duplicates removed\n",nedges);

  // make copy of graph for use in triangle finding

  MapReduce *mrcopy = new MapReduce(*mr);
  
  // augment edges with degree of each vertex
  // results in ((vi,vj),(deg(vi),deg(vj)) with all vi < vj

  mr->clone();
  mr->reduce(&emit_vertices,NULL);
  mr->collate(NULL);
  mr->reduce(&first_degree,NULL);
  mr->collate(NULL);
  mr->reduce(&second_degree,NULL);
  if (me == 0) printf("%d edges augmented by vertex degrees\n",nedges);

  // find triangles in degree-augmented graph and write to output file
  // once create angles, add in edges from original graph
  // this enables finding completed triangles in emit_triangles()
  // results in ((vi,vj,vk),None)

  mr->clone();
  mr->reduce(&low_degree,NULL);
  mr->collate(NULL);
  mr->reduce(&nsq_angles,NULL);
  mr->kv->add(mrcopy->kv);
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
  mr->clone();
  mr->reduce(&output_triangle,fp);
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
// all procs trim last line (between newline and NULL)
// 1st proc trims header line
// emit one KV per edge: key = (vi,vj), value = None

void fileread(int itask, char *str, int size, KeyValue *kv, void *ptr)
{
  /*
  lines = str.split("\n")
  lines.pop()
  if itask == 0: lines.pop(0)
  for line in lines:
    words = line.split()
    mr.add((int(words[0]),int(words[1])),None)
  */
}

// invert edges so all have vi < vj
// drop edges where vi = vj

void invert_edges(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  /*
  if key[0] < key[1]: mr.add((key[0],key[1]),None)
  elif key[1] < key[0]: mr.add((key[1],key[0]),None)
  */
}

// remove duplicate edges
// if I,J and J,I were in graph, now just one copy with I < J will be

void remove_duplicates(char *key, int keybytes, char *multivalue,
		       int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  //  mr.add(key,None)
}

// convert edge key to vertex keys
// emit two KV per edge: (vi,vj) and (vj,vi)

void emit_vertices(char *key, int keybytes, char *multivalue,
		   int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  //mr.add(key[0],key[1])
  //mr.add(key[1],key[0])
}

// assign degree of 1st vertex
// emit one KV per edge: ((vi,vj),(degree,0)) or ((vi,vj),(0,degree))
// where vi < vj and degree is assigned to correct vertex

void first_degree(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  /*
  degree = len(mvalue)
  for value in mvalue:
    if key < value: mr.add((key,value),(degree,0))
    else: mr.add((value,key),(0,degree))
  */
}

// assign degree of 2nd vertex
// 2 values per edge, with degree of vi and vj
// emit one KV per edge: ((vi,vj),(deg(i),deg(j))) with vi < vj

void second_degree(char *key, int keybytes, char *multivalue,
		   int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  //  if mvalue[0][0] != 0: mr.add(key,(mvalue[0][0],mvalue[1][1]))
  //else: mr.add(key,(mvalue[1][0],mvalue[0][1]))
}

// low-degree vertex emits (vi,vj)
// break tie with low-index vertex

void low_degree(char *key, int keybytes, char *multivalue,
		int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  //  di = mvalue[0][0]
  //dj = mvalue[0][1]
  //if di < dj: mr.add(key[0],key[1])
  //elif dj < di: mr.add(key[1],key[0])
  //elif key[0] < key[1]: mr.add(key[0],key[1])
  //else: mr.add(key[1],key[0])
}

// emit Nsq angles associated with each central vertex vi
// emit KV as ((vj,vk),vi) where vj < vk

void nsq_angles(char *key, int keybytes, char *multivalue,
		int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  /*
  n = len(mvalue)
  for i in range(n-1):
    for j in range(i+1,n):
      if mvalue[i] < mvalue[j]: mr.add((mvalue[i],mvalue[j]),key)
      else: mr.add((mvalue[j],mvalue[i]),key)
  */
}

// if None exists in mvalue, emit other values as triangles
// emit KV as ((vi,vj,vk),None)

void emit_triangles(char *key, int keybytes, char *multivalue,
		    int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  //  if None in mvalue:
  //  for value in mvalue:
  //    if value: mr.add((value,key[0],key[1]),None)
}

// print triangles to local file

void output_triangle(char *key, int keybytes, char *multivalue,
		     int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  //  print >>fp,key[0],key[1],key[2]
}
