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
//         -gb
//              Input file is in Greg Bayer's format, with each record
//              containing:
//                 32-bit timestamp
//                 32-bit time error
//                 64-bit from host ID
//                 64-bit from host path
//                 64-bit to host ID
//                 64-bit to host path
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
//         -ts timeseriesfile
//              Print time-series file format used by Greg Mackey.
//              Requires -c to convert hashkey IDs to integer IDs.
//              To keep time series, input file (in -ff or -f) should contain
//              only one file (one timestep).
//              A separate file per processor is produced for srcs and dests; 
//              these have to be concatenated for use.
//              timeseriesfile.srcs:  64-bit source vtx in range [0:n-1]
//              timeseriesfile.dests:  64-bit destination vtx in range [0:n-1]
//              timeseriesfile.vmap:  for each vtx in [0:n-1], 
//                                    64-bit host hashkey ID
//         -ff file
//              file with list of binary link files to read in (one per line)
//         -f file1 file2 ...
//              binary link files to read in
//              if specified, this must be the last switch used
//              Note that if k files are specified, at most k processors
//              initially have key-value pairs; this may lead to imbalanced
//              computation when k < nproc.

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.hpp"
#include "renumber_graph.hpp"
#include "read_fb_data.hpp"
#include "shared.hpp"

#include <map>

using namespace std;
using namespace MAPREDUCE_NS;

void edge_count(char *, int, char *, int, int *, KeyValue *, void *);
void edge_reverse(char *, int, char *, int, int *, KeyValue *, void *);
void edge_histo(char *, int, char *, int, int *, KeyValue *, void *);
int increasing_sort(char *, int, char *, int);
void key_value_reverse(char *, int, char *, int, int *, KeyValue *, void *);
void time_series_write(char *, int, char *, int, int *, KeyValue *, void *);
void time_series_vmap(char *, int, char *, int, int *, KeyValue *, void *);
int histo_sort(char *, int, char *, int);
void histo_write(char *, int, char *, int, int *, KeyValue *, void *);
void hfile_write(char *, int, char *, int, int *, KeyValue *, void *);
void matrix_write_inverse_degree(char *, int, char *, int, int *, KeyValue *, void *);
void matrix_write_weights(char *, int, char *, int, int *, KeyValue *, void *);

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

#ifdef NEW_OUT_OF_CORE
#ifdef ODIN
  // On odin, test the file system for writing; some nodes seem to have 
  // trouble writing to local disk.
  // This test currently uses an odin-specific path.  When MR-MPI allows
  // us to specify a filename prefix, we can make this test more generic.
  test_local_disk("/localdisk1/scratch");
#endif
#endif

  // parse command-line args

  char *tsfile = NULL;
  char *outhfile = NULL;
  char *inhfile = NULL;
  char *mfile = NULL;
  bool mfile_weights = 0;
  char *hfile = NULL;
  int convertflag = 0;

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
    } else if (strcmp(args[iarg],"-ts") == 0) {
      // Generate time-series file in Greg Mackey's format
      if (iarg+2 > narg) {
        flag = 1;
        break;
      }
      int n = strlen(args[iarg+1]) + 1;
      tsfile = new char[n];
      strcpy(tsfile,args[iarg+1]);
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
    } else {
      // Skip this argument; it may be meant for another subroutine.
      iarg += 1;
    }
  }

  if (flag) {
    if (me == 0)
      printf("Syntax: link2graph switch arg(s) switch arg(s) ...\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (convertflag == 0 && (outhfile || inhfile)) {
    if (me == 0) printf("Must convert vertex values if histogram\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (convertflag == 0 && tsfile) {
    if (me == 0) printf("Must convert vertex values if time-series file\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (convertflag == 0 && mfile) {
    if (me == 0) printf("Must convert vertex values if output matrix\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  // process the files and create a graph/matrix
  ReadFBData readFB(narg, args);

  if (readFB.vertexsize != 8 && tsfile) {
    if (me == 0) printf("Time-series files contain only hosts; use -e1\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  MapReduce *mrvert = NULL;
  MapReduce *mredge = NULL;
  uint64_t nverts;    // Number of unique non-zero vertices
  uint64_t nrawedges; // Number of edges in input files.
  uint64_t nedges;    // Number of unique edges in input files.
  readFB.run(&mrvert, &mredge, &nverts, &nrawedges, &nedges);

  // set nsingleton to -1 in case never compute it via options

  MPI_Barrier(MPI_COMM_WORLD);
  double tprev = MPI_Wtime();

  uint64_t nsingleton = -1;

  // output a hash-key file of unique edges I->J.
  // No header information; one chunk per proc.
  // Must do this before the convert, as convert re-writes mredges.
  if (hfile) {

    if (me == 0) {
      printf("Printing hash-key files ...\n");
      char fname[128];
      sprintf(fname,"%s.header",hfile);
      FILE *fp = fopen(fname,"w");
      fprintf(fp,"%ld %ld %ld\n",nverts,nverts,nedges);
      fclose(fp);
    }
    
    char fname[128];
    sprintf(fname,"%s.%d",hfile,me);
    FILE *fp = fopen(fname, "w");

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

  MPI_Barrier(MPI_COMM_WORLD);
  double tnow = MPI_Wtime();
  if (me == 0) printf("Time for hashfile:  %g secs\n", tnow - tprev);
  tprev = tnow;

  if (convertflag) {
    // update mrvert and mredge so their vertices are unique ints from 1-N,
    // not hash values
    renumber_graph(readFB.vertexsize, mrvert, mredge);
  } 

  MPI_Barrier(MPI_COMM_WORLD);
  tnow = MPI_Wtime();
  if (me == 0) printf("Time for convert:  %g secs\n", tnow - tprev);
  tprev = tnow;

  // output a time-series formatted file using Greg Mackey's format.
  if (tsfile) {
    if (me == 0) printf("Generating time-series file...\n");
    
    char fname[128];
    FILE *fp[2];
    sprintf(fname,"%s.srcs.%d",tsfile,me);
    fp[0] = fopen(fname,"w");
    sprintf(fname,"%s.dests.%d",tsfile,me);
    fp[1] = fopen(fname,"w");

    // Write the srcs and dests files, one per processor.
#ifdef NEW_OUT_OF_CORE
    MapReduce *mrout = mredge->copy();
#else
    MapReduce *mrout = new MapReduce(*mredge);
#endif
    mrout->convert();
    mrout->reduce(&time_series_write,fp);
    
    delete mrout;
    fclose(fp[0]);
    fclose(fp[1]);

    // Write the vmap file:  gather to processor 0, sort, output to single file.
    if (me == 0) {
      sprintf(fname, "%s.vmap", tsfile);
      fp[0] = fopen(fname,"w");
    } else fp[0] = NULL;
    
#ifdef NEW_OUT_OF_CORE
    mrout = mrvert->copy();
#else
    mrout = new MapReduce(*mrvert);
#endif
   
    mrout->clone();
    mrout->reduce(key_value_reverse,NULL);
    mrout->gather(1);
    mrout->sort_keys(&increasing_sort);
    mrout->clone();
    mrout->reduce(&time_series_vmap,fp[0]);

    delete mrout;
    fclose(fp[0]);
  }
  if (mrvert) delete mrvert;

  MPI_Barrier(MPI_COMM_WORLD);
  tnow = MPI_Wtime();
  if (me == 0) printf("Time for tsfile:  %g secs\n", tnow - tprev);
  tprev = tnow;

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
    uint64_t n = mrdegree->reduce(&edge_count,NULL);
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
      fprintf(fp,"%d %ld\n",0,nsingleton);
      fclose(fp);
    }
  }

  // compute and output an in-degree histogram

  if (inhfile) {

    // mrdegree = vertices with their in degree as negative value
    // nsingleton_in = # of verts with 0 indegree
    
    if (me == 0) printf("Generating in-degree histogram...\n");
#ifdef NEW_OUT_OF_CORE
    MapReduce *mrdegree = mredge->copy();
#else
    MapReduce *mrdegree = new MapReduce(*mredge);
#endif
    mrdegree->clone();
    mrdegree->reduce(&edge_reverse,NULL);  // Assumes destination vertex is
                                           // first field in EDGE struct.
    mrdegree->collate(NULL);
    uint64_t n = mrdegree->reduce(&edge_count,NULL);
    uint64_t nsingleton_in = nverts - n;

    // mrhisto KV = (in degree, vert count)
    
    MapReduce *mrhisto = mrdegree;
    mrhisto->clone();
    mrhisto->reduce(&edge_reverse,NULL);
    mrhisto->collate(NULL);
    mrhisto->reduce(&edge_histo,NULL);

    // output sorted histogram of in degree
    // add inferred zero-degree as last entry

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
      fprintf(fp,"%d %ld\n",0,nsingleton_in);
      fclose(fp);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  tnow = MPI_Wtime();
  if (me == 0) printf("Time for histos:  %g secs\n", tnow - tprev);
  tprev = tnow;

  // output a Matrix Market file
  // one-line header + one chunk per proc

  if (mfile) {

    if (me == 0) printf("Generating matrix-market file...\n");
    if (me == 0) {
      char fname[128];
      sprintf(fname,"%s.header",mfile);
      FILE *fp = fopen(fname,"w");
      fprintf(fp,"%ld %ld %ld\n",nverts,nverts,nedges);
      fclose(fp);
    }
    
    char fname[128];
    sprintf(fname,"%s.%d",mfile,me);
    FILE *fp = fopen(fname, "w");

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
      uint64_t n = mrdegree->reduce(&edge_count,NULL);
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

  MPI_Barrier(MPI_COMM_WORLD);
  tnow = MPI_Wtime();
  if (me == 0) printf("Time for mtx:  %g secs\n", tnow - tprev);
  tprev = tnow;

  // clean up

  delete [] outhfile;
  delete [] inhfile;
  delete [] mfile;

  delete mredge;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0) {
    printf("Graph: %ld original edges\n",nrawedges);
    printf("Graph: %ld unique vertices\n",nverts);
    printf("Graph: %ld unique edges\n",nedges);
    printf("Graph: %ld vertices with zero out-degree\n",nsingleton);
    printf("Time for map:      %g secs\n",readFB.timeMap);
    printf("Time for unique:   %g secs\n",readFB.timeUnique);
    printf("Time without map:  %g secs\n",tstop-tstart-readFB.timeMap);
    printf("Total Time:        %g secs\n",tstop-tstart);
  }

  MPI_Finalize();
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

  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  if (keybytes == 16) {
    // Two 64-bit ints per vertex
    VERTEX16 *vi = (VERTEX16 *) key;
    EDGE16 *edge = (EDGE16 *) multivalue;
    for (i = 0; i < nvalues; i++)
      fprintf(fp, "%lld %lld   %lld %lld %d\n",
              vi->v[0], vi->v[1], edge[i].v.v[0], edge[i].v.v[1], edge[i].wt);
  }
  else if (keybytes == 8) {
    // One 64-bit int per vertex
    VERTEX08 *vi = (VERTEX08 *) key;
    EDGE08 *edge = (EDGE08 *) multivalue;
    for (i = 0; i < nvalues; i++)
      fprintf(fp, "%lld   %lld %d\n", vi->v[0], edge[i].v.v[0], edge[i].wt);
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
  iVERTEX *vi = (iVERTEX *) key;

  // First, find the negative int, which is -degree of Vi.
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)
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
      iEDGE *edge = (iEDGE *) &multivalue[offset];
      fprintf(fp, "%d %d %f\n", vi->v, edge->v.v, inverse_outdegree);
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
  iVERTEX *vi = (iVERTEX *) key;

  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  iEDGE *edge = (iEDGE *) multivalue;

  for (i = 0; i < nvalues; i++) {
    fprintf(fp, "%d %d %d.\n", vi->v, edge->v.v, edge->wt);
    edge++;
  }

  END_BLOCK_LOOP
}

/* ----------------------------------------------------------------------
   increasing_sort compare() function
   sort values in increasing order
------------------------------------------------------------------------- */

int increasing_sort(char *value1, int len1, char *value2, int len2)
{
  int *i1 = (int *) value1;
  int *i2 = (int *) value2;
  if (*i1 < *i2) return -1;
  else if (*i1 > *i2) return 1;
  else return 0;
}

/* ----------------------------------------------------------------------
   key_value_reverse reduce() function
   input KMV: (key, value)
   output KV: (value, key)
------------------------------------------------------------------------- */

void key_value_reverse(char *key, int keybytes, char *multivalue,
                       int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  if (nvalues > 1) {
    printf("Improper use of key_value_reverse; nvalues = %d > 1.\n", nvalues);
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  if (!multivalue) {
    printf("Improper use of key_value_reverse; multivalue == NULL.\n");
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  kv->add(multivalue,valuebytes[0],key,keybytes);
}

/* ----------------------------------------------------------------------
   time_series_write reduce() function
   input KMV: (Vi,[{Vj Wj} {Vk Wk} ...]), 
   where Wj is the weight of edge Vi->Vj.
   write each edge to files, create no new KV
------------------------------------------------------------------------- */
void time_series_write(char *key, int keybytes, char *multivalue,
                  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int i;
  FILE **fp = (FILE **) ptr;
  iVERTEX vi = *((iVERTEX *) key);

  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  iEDGE *edge = (iEDGE *) multivalue;

  uint64_t vi_l = (uint64_t) vi.v-1;  // Greg Mackey's format is [0:N-1]
  for (i = 0; i < nvalues; i++) {
    uint64_t vj_l = (uint64_t) edge[i].v.v-1; // Greg Mackey's format is [0:N-1]
    fwrite(&vi_l, 8, 1, fp[0]);
    fwrite(&vj_l, 8, 1, fp[1]);
  }

  END_BLOCK_LOOP
}

/* ----------------------------------------------------------------------
   time_series_vmap reduce() function
   input KMV: (Vi (in [1:N],Vi in hashkey ID) 
   Keys are sorted.
   write hashkey ID to file, create no new KV
------------------------------------------------------------------------- */

void time_series_vmap(char *key, int keybytes, char *multivalue,
                      int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  FILE *fp = (FILE *) ptr;

  uint64_t *hashkey = (uint64_t *) multivalue;
  fwrite(hashkey, 8, 1, fp);
}

