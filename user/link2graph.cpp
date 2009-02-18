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
void cull(char *, int, char *, int, int *, KeyValue *, void *);

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

  MapReduce *mrmaster = new MapReduce(MPI_COMM_WORLD);
  mrmaster->verbosity = 2;

  int nrecords = mrmaster->map(narg-1,&fileread,&args[1]);
  int nverts = mrmaster->collate(NULL);
  int nedges = mrmaster->reduce(&cull,NULL);




  MapReduce *mr = new MapReduce(*mrmaster);

  //mr->gather(1);
  //mr->sort_values(&ncompare);
  //mr->clone();

  /*
  FILE *fp = NULL;
  if (me == 0) {
    fp = fopen("tmp.out","w");
    fprintf(fp,"# %d unique words\n",nunique);
  }
  mr->reduce(&output,fp);
  if (me == 0) fclose(fp);
  */

  delete mr;
  delete mrmaster;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0) {
    printf("Graph: %d original edges\n",nrecords/2);
    printf("Graph: %d vertices\n",nverts);
    printf("Graph: %d edges\n",nedges/2);
  }

  MPI_Finalize();
}

/* ----------------------------------------------------------------------
   fileread map() function
   for each record in file, grab "from host" and "to host" fields
   emit as two graph edges: I,J and J,I
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
      kv->add(&ptr[16],8,&ptr[0],8);
      ptr += RECORDSIZE;
    }
    if (nrecords == 0) break;
  }

  fclose(fp);
}

/* ----------------------------------------------------------------------
   cull reduce() function
   input KMV: key = unique vertex, values = all edges, including 0 and dups
   output KV: key = unique vertex, values = unique non-zero edges
------------------------------------------------------------------------- */

void cull(char *key, int keybytes, char *multivalue,
	  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  std::map<unsigned long,int> hash;

  unsigned long *vertex = (unsigned long *) multivalue;
  for (int i = 0; i < nvalues; i++) {
    if (vertex[i] == 0) continue;
    if (hash.find(vertex[i]) == hash.end()) hash[vertex[i]] = 0;
    else continue;
    kv->add(key,keybytes,(char *) &vertex[i],VERTEXSIZE);
  }
}
