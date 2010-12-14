// Benchmark to enumerate sub-graph isomorphism matches in undirected graphs
//
// Syntax: sgi_driver -efile file1 flag -vfile file2
//                    -n 10 -e 8 -abcd 0.25 0.25 0.25 0.25 -f 0.8 
//                    -s 587283 -vatt 3 5 -eatt 1 1
//                    -g 3 3 0 1 4 0 1 4 1
//                    -o out.efile out.vfile
//         -efile = file name of edges and edge attributes to read in MM format
//                  flag = 0 for no edge wts, 1 for integer, 2 for double
//         -vfile = file name of vertices and attributes to read in
//         -n = order of matrix = 2^n
//         -e = # of edges per row
//         -abcd = RMAT a,b,c,d params, default = 0.25 for each
//         -f = 0.0 <= fraction < 1.0, default = 0.0
//         -s = random # seed = positive int, default = 12345
//         -vatt = lo hi = range of vertex attributes, lo to hi inclusive
//         -eatt = lo hi = range of edge attributes, lo to hi inclusive
//         -g = N V0 F0 E01 V1 F1 E12 V2 F2 ... En-2n-1 Vn-1 Fn-1
//              definition of target search graph
//              N = # of vertices in tour of target graph
//              V0...Vn-1 = attribute of each vertex in tour
//              E01...En-2n-1 = attribute of each edge in tour
//              F0...Fn-1 = 0 to N-1 if Vi is same as earlier Vf
//              F0...Fn-1 = -1 if Vi is different than all earier vertices
//         -m = -1 to enumerate all SGI matches
//              0 to count all SGI matches
//              M > 1 to count and them sample M SGI matches
//         -o = out.efile out.vfile
//              write out 2 files for edges and vertices with attributes
//              useful when auto-generated with -n and -e

// either efile/vfile or n/e switches are required
// -vatt and -eatt switches are required
// -g and -m switches are required
// vertex and edge attributes are assigned randomly to RMAT graphs

#include "mpi.h"
#include "stdio.h"
#include "stdint.h"
#include "string.h"
#include "stdlib.h"
#include "read_mm_edge.h"
#include "read_mm_vert.h"
#include "rmat_generate.h"
#include "matrix_upper.h"
#include "vert_generate.h"
#include "graph_label.h"
#include "sgi_enumerate.h"
#include "sgi_sample.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

struct Params {
  int me;
  char *efile,*vfile;
  int edgeflag;
  uint64_t nvert,nedge;
  double a,b,c,d;
  double fraction;
  int seed;
  int vatt,eatt;
  int vattlo,vatthi;
  int eattlo,eatthi;
  int ntour;
  int *vtour,*ftour,*etour;
  int mlimit;
  char *outefile,*outvfile;
  FILE *fp;
};

void parse(int narg, char **args, Params *);
void eprint(uint64_t, char *, int, char *, int,
	    MAPREDUCE_NS::KeyValue *, void *);
void vprint(uint64_t, char *, int, char *, int,
	    MAPREDUCE_NS::KeyValue *, void *);

typedef uint64_t VERTEX;
typedef int LABEL;
typedef struct {
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
  
  Params in;
  parse(narg,args,&in);
  in.me = me;

  // MRV for graph vertices, MRE for graph edges

  MapReduce *mrv = new MapReduce(MPI_COMM_WORLD);
  mrv->verbosity = 0;
  mrv->timer = 0;
  MapReduce *mre = new MapReduce(MPI_COMM_WORLD);
  mre->verbosity = 0;
  mre->timer = 0;

  //mrv->memsize = 1;
  //mre->memsize = 1;

  // populate MRV,MVE with labeled graph
  // MRE = Eij : Fij
  // MRV = Vi : Wi
  // either:
  // (a) read MM file with or without edge weights and read/generate vertices
  // (b) generate RMAT graph
  // for MM file, assume no self or duplicate edges
  // for RMAT, force matrix to be upper triangular with no self-edges

  if (in.efile) {
    ReadMMEdge readmme(in.efile,in.edgeflag,MPI_COMM_WORLD);
    readmme.run(mre,in.nvert,in.nedge);

    if (in.edgeflag == 0) {
      GraphLabel elabel(in.eattlo,in.eatthi,in.seed,MPI_COMM_WORLD);
      elabel.run(mre);
    }

    if (in.vfile) {
      uint64_t tmp;
      ReadMMVert readmmv(in.vfile,1,MPI_COMM_WORLD);
      readmmv.run(mrv,tmp);
    } else {
      VertGenerate vgen(in.nvert,0,MPI_COMM_WORLD);
      vgen.run(mrv);

      GraphLabel vlabel(in.vattlo,in.vatthi,in.seed+10000,MPI_COMM_WORLD);
      vlabel.run(mrv);
    }

  } else {
    int niterate;
    RMATGenerate rmat(in.nvert,in.nedge,
		      in.a,in.b,in.c,in.d,in.fraction,in.seed,MPI_COMM_WORLD);
    rmat.run(mre,niterate);

    MatrixUpper upper(MPI_COMM_WORLD);
    upper.run(mre,in.nedge);

    VertGenerate vgen(in.nvert,0,MPI_COMM_WORLD);
    vgen.run(mrv);

    GraphLabel vlabel(in.vattlo,in.vatthi,in.seed+10000,MPI_COMM_WORLD);
    vlabel.run(mrv);

    GraphLabel elabel(in.eattlo,in.eatthi,in.seed+20000,MPI_COMM_WORLD);
    elabel.run(mre);
  }

  // output edge and vertex files

  if (in.outefile) {
    in.fp = NULL;
    mre->map(mre,eprint,&in,1);
    if (in.fp) fclose(in.fp);
    in.fp = NULL;
    mrv->map(mrv,vprint,&in,1);
    if (in.fp) fclose(in.fp);
  }

  //if (me == 0) printf("VERTICES\n");
  //mrv->print(-1,1,2,1);
  //if (me == 0) printf("EDGES\n");
  //mre->print(-1,1,7,1);

  // SGI to enumerate sub-graph matches

  if (in.mlimit < 0) {
    MapReduce *mrs = new MapReduce(MPI_COMM_WORLD);
    mrs->verbosity = 0;
    mrs->timer = 0;
    MapReduce *mrx = new MapReduce(MPI_COMM_WORLD);
    mrx->verbosity = 0;
    mrx->timer = 0;
    MapReduce *mry = new MapReduce(MPI_COMM_WORLD);
    mry->verbosity = 0;
    mry->timer = 0;

    //mrs->memsize = 1;
    //mrx->memsize = 1;
    //mry->memsize = 1;

    uint64_t nsgi;
    SGIEnumerate sgi(in.ntour,in.vtour,in.ftour,in.etour,MPI_COMM_WORLD);
    double time = sgi.run(mrv,mre,mrs,mrx,mry,nsgi);

    if (me == 0)
      printf("SGI enumerate: %g secs, %u SG in big graph with "
	     "%u verts, %u edges on %d procs\n",time,nsgi,
	     in.nvert,in.nedge,nprocs);

    delete mrs;
    delete mrx;
    delete mry;

  } else {
    MapReduce *mrs = new MapReduce(MPI_COMM_WORLD);
    mrs->verbosity = 0;
    mrs->timer = 0;
    MapReduce *mrx = new MapReduce(MPI_COMM_WORLD);
    mrx->verbosity = 0;
    mrx->timer = 0;
    MapReduce *mry = new MapReduce(MPI_COMM_WORLD);
    mry->verbosity = 0;
    mry->timer = 0;

    uint64_t nsgi;
    SGISample sgi(in.mlimit,in.ntour,in.vtour,in.ftour,in.etour,MPI_COMM_WORLD);
    double time = sgi.run(mrv,mre,mrs,mrx,mry,nsgi);

    if (me == 0)
      printf("SGI sample: %g secs, %u SG in big graph with "
	     "%u verts, %u edges on %d procs\n",time,nsgi,
	     in.nvert,in.nedge,nprocs);

    delete mrs;
    delete mrx;
    delete mry;
  }


  // clean up

  delete mrv;
  delete mre;
  delete [] in.efile;
  delete [] in.vfile;
  delete [] in.vtour;
  delete [] in.ftour;
  delete [] in.etour;
  MPI_Finalize();
}

/* ---------------------------------------------------------------------- */

void parse(int narg, char **args, Params *in)
{
  in->efile = in->vfile = NULL;
  in->nvert = in->nedge = 0;
  in->a = in->b = in->c = in->d = 0.25;
  in->fraction = 0.0;
  in->seed = 12345;
  in->vatt = in->eatt = 0;
  in->ntour = 0;
  in->mlimit = -2;
  in->outefile = in->outvfile = NULL;

  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-efile") == 0) {
      if (iarg+3 > narg) break;
      int n = strlen(args[iarg+1]) + 1;
      delete [] in->efile;
      in->efile = new char[n];
      strcpy(in->efile,args[iarg+1]);
      in->edgeflag = atoi(args[iarg+2]);
      iarg += 3;
    } else if (strcmp(args[iarg],"-vfile") == 0) {
      if (iarg+2 > narg) break;
      int n = strlen(args[iarg+1]) + 1;
      delete [] in->vfile;
      in->vfile = new char[n];
      strcpy(in->vfile,args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-n") == 0) {
      if (iarg+2 > narg) break;
      int order = atoi(args[iarg+1]);
      uint64_t one = 1;
      in->nvert = one << order;
      iarg += 2;
    } else if (strcmp(args[iarg],"-e") == 0) {
      if (iarg+2 > narg) break;
      in->nedge = atoi(args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-abcd") == 0) {
      if (iarg+5 > narg) break;
      in->a = atof(args[iarg+1]);
      in->b = atof(args[iarg+2]);
      in->c = atof(args[iarg+3]);
      in->d = atof(args[iarg+4]);
      iarg += 5;
    } else if (strcmp(args[iarg],"-f") == 0) {
      if (iarg+2 > narg) break;
      in->fraction = atof(args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-s") == 0) {
      if (iarg+2 > narg) break;
      in->seed = atoi(args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-vatt") == 0) {
      if (iarg+3 > narg) break;
      in->vatt = 1;
      in->vattlo = atoi(args[iarg+1]);
      in->vatthi = atoi(args[iarg+2]);
      iarg += 3;
    } else if (strcmp(args[iarg],"-eatt") == 0) {
      if (iarg+3 > narg) break;
      in->eatt = 1;
      in->eattlo = atoi(args[iarg+1]);
      in->eatthi = atoi(args[iarg+2]);
      iarg += 3;
    } else if (strcmp(args[iarg],"-g") == 0) {
      if (iarg+2 > narg) break;
      in->ntour = atoi(args[iarg+1]);
      if (in->ntour < 1) break;
      if (iarg+3*in->ntour > narg) break;
      in->vtour = new int[in->ntour];
      in->ftour = new int[in->ntour];
      in->etour = new int[in->ntour];
      for (int i = 0; i < in->ntour; i++) {
	in->vtour[i] = atoi(args[iarg + 3*i + 2]);
	in->ftour[i] = atoi(args[iarg + 3*i + 3]);
	if (i != in->ntour-1) in->etour[i] = atoi(args[iarg + 3*i + 4]);
      }
      iarg += 3*in->ntour + 1;
    } else if (strcmp(args[iarg],"-m") == 0) {
      if (iarg+2 > narg) break;
      in->mlimit = atoi(args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-o") == 0) {
      if (iarg+3 > narg) break;
      int n = strlen(args[iarg+1]) + 1;
      delete [] in->outefile;
      in->outefile = new char[n];
      strcpy(in->outefile,args[iarg+1]);
      n = strlen(args[iarg+2]) + 1;
      delete [] in->outvfile;
      in->outvfile = new char[n];
      strcpy(in->outvfile,args[iarg+2]);
      iarg += 3;
    } else break;
  }

  int me;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);

  if (iarg < narg) {
    if (me == 0) printf("ERROR: Invalid command-line args\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  int rmatflag = 0;
  if (in->nvert && in->nedge) rmatflag = 1;

  if (!in->efile && !rmatflag) {
    if (me == 0) printf("ERROR: No command-line setting for files or RMAT\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if ((in->efile || in->vfile) && rmatflag) {
    if (me == 0)
      printf("ERROR: Ccommand-line setting for both files and RMAT\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (in->efile && in->edgeflag && in->eatt) {
    if (me == 0) printf("ERROR: Edge attributes both in file and via eatt");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (in->efile && in->edgeflag == 0 && in->eatt == 0) {
    if (me == 0) printf("ERROR: No edge attributes defined");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (in->vfile && in->vatt) {
    if (me == 0) printf("ERROR: Vertex attributes both in file and via vatt");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (rmatflag && (in->vatt == 0 || in->eatt == 0)) {
    if (me == 0) printf("ERROR: No command-line setting for -vatt or -eatt\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (rmatflag && (in->vattlo > in->vatthi)) {
    if (me == 0) printf("ERROR: Invalid command-line setting for -vatt\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (rmatflag && (in->eattlo > in->eatthi)) {
    if (me == 0) printf("ERROR: Invalid command-line setting for -eatt\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (in->ntour == 0) {
    if (me == 0) printf("ERROR: No command-line setting for -g\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  for (int i = 0; i < in->ntour; i++) {
    if (rmatflag && (in->vtour[i] < in->vattlo || 
		     in->vtour[i] > in->vatthi)) {
      if (me == 0) printf("ERROR: Invalid command-line setting for -g\n");
      MPI_Abort(MPI_COMM_WORLD,1);
    }
    if (in->ftour[i] >= i) {
      if (me == 0) printf("ERROR: Invalid command-line setting for -g\n");
      MPI_Abort(MPI_COMM_WORLD,1);
    }
    if (i != in->ntour-1) {
      if (rmatflag && (in->etour[i] < in->eattlo || 
		       in->etour[i] > in->eatthi)) {
	if (me == 0) printf("ERROR: Invalid command-line setting for -g\n");
	MPI_Abort(MPI_COMM_WORLD,1);
      }
    }
  }

  if (in->mlimit == -2) {
    if (me == 0) printf("ERROR: No command-line setting for -m\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (in->mlimit < -1) {
    if (me == 0) printf("ERROR: Invalid command-line setting for -m\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (in->nvert) in->nedge *= in->nvert;
}

/* ---------------------------------------------------------------------- */

void eprint(uint64_t itask, char *key, int keybytes, 
	    char *value, int valuebytes, KeyValue *kv, void *ptr) 
{
  Params *in = (Params *) ptr;

  if (itask == 0) {
    char fname[16];
    sprintf(fname,"%s.%d",in->outefile,in->me);
    in->fp = fopen(fname,"w");
  }

  EDGE *edge = (EDGE *) key;
  LABEL *fij = (LABEL *) value;
  fprintf(in->fp,"%u %u %d\n",edge->vi,edge->vj,*fij);
}

/* ---------------------------------------------------------------------- */

void vprint(uint64_t itask, char *key, int keybytes, 
	    char *value, int valuebytes, KeyValue *kv, void *ptr) 
{
  Params *in = (Params *) ptr;

  if (itask == 0) {
    char fname[16];
    sprintf(fname,"%s.%d",in->outvfile,in->me);
    in->fp = fopen(fname,"w");
  }

  VERTEX *vi = (VERTEX *) key;
  LABEL *wi = (LABEL *) value;
  fprintf(in->fp,"%u %d\n",*vi,*wi);
}
