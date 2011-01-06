// Serial check to enumerate sub-graph isomorphism matches in undirected graphs
//   enumerate via brute-force recursive strategy
//
// Syntax: sgi_check -efile file1 nprocs -vfile nprocs file2 -o out.sgi
//                    -g 3 3 0 1 4 0 1 4 1
//         -efile = file name of edges and edge attributes to read in MM format
//                  also number of procs it was written by
//         -vfile = file name of vertices and attributes to read in
//                  also number of procs it was written by
//         -o = file name to write out with SGI matches
//         -g = N V0 F0 E01 V1 F1 E12 V2 F2 ... En-2n-1 Vn-1 Fn-1
//              definition of target search graph
//              N = # of vertices in tour of target graph
//              V0...Vn-1 = attribute of each vertex in tour
//              E01...En-2n-1 = attribute of each edge in tour
//              F0...Fn-1 = 0 to N-1 if Vi is same as earlier Vf
//              F0...Fn-1 = -1 if Vi is different than all earier vertices

#include "mpi.h"
#include "stdio.h"
#include "stdint.h"
#include "string.h"
#include "stdlib.h"

struct Params {
  char *efile,*vfile;
  int eprocs,vprocs;
  char *outfile;
  int ntour;
  int *vtour,*ftour,*etour;
  FILE *fp;
};

typedef uint64_t VERTEX;
typedef int LABEL;
typedef struct {
  VERTEX v;
  LABEL w;
} VERT;
typedef struct {
  VERTEX vi,vj;
  LABEL fij,wi,wj;
} EDGE;


void parse(int narg, char **args, Params *);
int read_verts(char *, int, VERT **);
int read_edges(char *, int, EDGE **);
void combine(int, VERT *, int, EDGE *);
void find(int, int, VERT *, int, EDGE *, 
	  int, int *, int *, int *, int &, VERTEX *, FILE *);

#define MAXLINE 80
#define CHUNK 1

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  // parse command-line args
  
  Params in;
  parse(narg,args,&in);

  // read files of edges and vertices

  VERT *verts = NULL;
  int nvert = read_verts(in.vfile,in.vprocs,&verts);

  EDGE *edges = NULL;
  int nedge = read_edges(in.efile,in.eprocs,&edges);
  
  combine(nvert,verts,nedge,edges);

  //for (int i = 0; i < nedge; i++)
  //  printf("EDGE %lu %lu: %d %d %d\n",
  //	   edges[i].vi,edges[i].vj,edges[i].fij,edges[i].wi,edges[i].wj);

  // recursive find of each SGI match

  double tstart = MPI_Wtime();

  int nsgi = 0;
  VERTEX *tour = new VERTEX[in.ntour];
  FILE *fp = fopen(in.outfile,"w");

  find(0,nvert,verts,nedge,edges,
       in.ntour,in.vtour,in.ftour,in.etour,nsgi,tour,fp);

  fclose(fp);

  double tstop = MPI_Wtime();
  double time = tstop-tstart;

  // stats

  printf("SGI find: %g secs, %d SG in big graph with "
	 "%d verts, %d edges\n",time,nsgi,nvert,nedge);

  // clean up

  free(verts);
  free(edges);
  delete [] in.efile;
  delete [] in.vfile;
  delete [] in.outfile;
  delete [] in.vtour;
  delete [] in.ftour;
  delete [] in.etour;
  MPI_Finalize();
}

/* ---------------------------------------------------------------------- */

void find(int itour, int nvert, VERT *verts, int nedge, EDGE *edges, 
	  int ntour, int *vtour, int *ftour, int *etour,
	  int &nsgi, VERTEX *tour, FILE *fp)
{
  // tour is complete, print it out

  if (itour == ntour) {
    for (int i = 0; i < ntour; i++)
      fprintf(fp,"%lu ",tour[i]);
    fprintf(fp,"\n");
    nsgi++;
    return;
  }

  // 1st vertex

  if (itour == 0) {
    for (int i = 0; i < nvert; i++)
      if (verts[i].w == vtour[itour]) {
	tour[itour] = verts[i].v;
	find(itour+1,nvert,verts,nedge,edges,
	     ntour,vtour,ftour,etour,nsgi,tour,fp);
      }

  // subsequent edges and vertices, match in either direction

  } else {
    int flag = ftour[itour];
    for (int i = 0; i < nedge; i++) {
      if (edges[i].vi == tour[itour-1] &&
	  edges[i].wi == vtour[itour-1] && 
	  edges[i].fij == etour[itour-1] && 
	  edges[i].wj == vtour[itour]) {
	if (flag < 0) {
	  int j;
	  for (j = 0; j < itour; j++)
	    if (edges[i].vj == tour[j]) break;
	  if (j == itour) {
	    tour[itour] = edges[i].vj;
	    find(itour+1,nvert,verts,nedge,edges,
		 ntour,vtour,ftour,etour,nsgi,tour,fp);
	  }
	} else {
	  if (edges[i].vj == tour[flag]) {
	    tour[itour] = edges[i].vj;
	    find(itour+1,nvert,verts,nedge,edges,
		 ntour,vtour,ftour,etour,nsgi,tour,fp);
	  }
	}
      }
      if (edges[i].vj == tour[itour-1] &&
	  edges[i].wj == vtour[itour-1] && 
	  edges[i].fij == etour[itour-1] && 
	  edges[i].wi == vtour[itour]) {
	if (flag < 0) {
	  int j;
	  for (j = 0; j < itour; j++)
	    if (edges[i].vi == tour[j]) break;
	  if (j == itour) {
	    tour[itour] = edges[i].vi;
	    find(itour+1,nvert,verts,nedge,edges,
		 ntour,vtour,ftour,etour,nsgi,tour,fp);
	  }
	} else {
	  if (edges[i].vi == tour[flag]) {
	    tour[itour] = edges[i].vi;
	    find(itour+1,nvert,verts,nedge,edges,
		 ntour,vtour,ftour,etour,nsgi,tour,fp);
	  }
	}
      }
    }
  }
}

/* ---------------------------------------------------------------------- */

int read_verts(char *file, int nprocs, VERT **pverts)
{
  char line[MAXLINE];

  // read files one at a time

  int nvert = 0;
  int maxvert = 0;
  VERT *verts = NULL;

  for (int i = 0; i < nprocs; i++) {
    char fname[16];
    sprintf(fname,"%s.%d",file,i);
    FILE *fp = fopen(fname,"r");

    VERTEX v;
    LABEL w;
    while (fgets(line,MAXLINE,fp)) {
      sscanf(line,"%ld %d",&v,&w);
      if (nvert == maxvert) {
	maxvert += CHUNK;
	verts = (VERT *) realloc(verts,maxvert*sizeof(VERT));
      }
      verts[nvert].v = v;
      verts[nvert].w = w;
      nvert++;
    }
    
    fclose(fp);
  }

  *pverts = verts;
  return nvert;
}

/* ---------------------------------------------------------------------- */

int read_edges(char *file, int nprocs, EDGE **pedges)
{
  char line[MAXLINE];

  // read files one at a time

  int nedge = 0;
  int maxedge = 0;
  EDGE *edges = NULL;

  for (int i = 0; i < nprocs; i++) {
    char fname[16];
    sprintf(fname,"%s.%d",file,i);
    FILE *fp = fopen(fname,"r");

    EDGE e;
    LABEL fij;
    while (fgets(line,MAXLINE,fp)) {
      sscanf(line,"%ld %ld %d",&e.vi,&e.vj,&fij);
      if (nedge == maxedge) {
	maxedge += CHUNK;
	edges = (EDGE *) realloc(edges,maxedge*sizeof(EDGE));
      }
      edges[nedge].vi = e.vi;
      edges[nedge].vj = e.vj;
      edges[nedge].fij = fij;
      nedge++;
    }
    
    fclose(fp);
  }

  *pedges = edges;
  return nedge;
}

/* ---------------------------------------------------------------------- */

void combine(int nvert, VERT *verts, int nedge, EDGE *edges)
{
  for (int i = 0; i < nedge; i++) {
    VERTEX v = edges[i].vi;
    for (int j = 0; j < nvert; j++)
      if (v == verts[j].v) {
	edges[i].wi = verts[j].w;
	break;
      }
    v = edges[i].vj;
    for (int j = 0; j < nvert; j++)
      if (v == verts[j].v) {
	edges[i].wj = verts[j].w;
	break;
      }
  }
}

/* ---------------------------------------------------------------------- */

void parse(int narg, char **args, Params *in)
{
  in->efile = in->vfile = NULL;
  in->outfile = NULL;
  in->ntour = 0;

  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-efile") == 0) {
      if (iarg+3 > narg) break;
      int n = strlen(args[iarg+1]) + 1;
      delete [] in->efile;
      in->efile = new char[n];
      strcpy(in->efile,args[iarg+1]);
      in->eprocs = atoi(args[iarg+2]);
      iarg += 3;
    } else if (strcmp(args[iarg],"-vfile") == 0) {
      if (iarg+3 > narg) break;
      int n = strlen(args[iarg+1]) + 1;
      delete [] in->vfile;
      in->vfile = new char[n];
      strcpy(in->vfile,args[iarg+1]);
      in->vprocs = atoi(args[iarg+2]);
      iarg += 3;
    } else if (strcmp(args[iarg],"-o") == 0) {
      if (iarg+2 > narg) break;
      int n = strlen(args[iarg+1]) + 1;
      delete [] in->outfile;
      in->outfile = new char[n];
      strcpy(in->outfile,args[iarg+1]);
      iarg += 2;
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
    } else break;
  }

  if (iarg < narg) {
    printf("ERROR: Invalid command-line args\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (in->efile == NULL || in->vfile == NULL || in->outfile == NULL) {
    printf("ERROR: No command-line setting for -efile or -vfile or -o\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  if (in->ntour == 0) {
    printf("ERROR: No command-line setting for -g\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }

  for (int i = 0; i < in->ntour; i++) {
    if (in->ftour[i] >= i) {
      printf("ERROR: Invalid command-line setting for -g\n");
      MPI_Abort(MPI_COMM_WORLD,1);
    }
  }
}
