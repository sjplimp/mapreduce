/* ----------------------------------------------------------------------
   read a Matrix Market edge file with or without edge weights
   file = file to read
   wtflag = 0 for ignore edge weights, 1 for integer weights, 2 for double
   input MR = empty
   aggregate edges to owning processor
   output MR = Eij : wt, Eij = Vi Vj
   nvert = # of vertices (from header of MM file)
   nedge = # of edges
   datatypes: Vi = uint64, wt = NULL or int or double
 ------------------------------------------------------------------------- */

#include "mpi.h"
#include "string.h"
#include "stdlib.h"
#include "read_mm_edge.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReadMMEdge::ReadMMEdge(char *file_in, int wtflag_in, MPI_Comm world_in)
{
  file = file_in;
  wtflag = wtflag_in;
  world = world_in;
}

/* ---------------------------------------------------------------------- */

double ReadMMEdge::run(MapReduce *mr, uint64_t &nvert, uint64_t &nedge)
{
  int nprocs;
  MPI_Comm_size(world,&nprocs);

  MPI_Barrier(world);
  double tstart = MPI_Wtime();

  nv = 0;
  nedge = mr->map(nprocs,1,&file,'\n',80,map,this);

  int nvsum;
  MPI_Allreduce(&nv,&nvsum,1,MPI_INT,MPI_SUM,world);
  nvert = nvsum;

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  return tstop - tstart;
}

/* ---------------------------------------------------------------------- */

void ReadMMEdge::map(int itask, char *bytes, int nbytes, 
		     KeyValue *kv, void *ptr)
{
  EDGE edge;
  int vi,vj,iwt;
  double dwt;
  IWEIGHT iweight;
  DWEIGHT dweight;
  char *next;

  ReadMMEdge *rmm = (ReadMMEdge *) ptr;
  int wtflag = rmm->wtflag;

  int skip = 0;

  while (1) {
    next = strchr(bytes,'\n');
    if (!next) break;

    if (bytes[0] == '%') skip = 1;
    else if (skip) {
      sscanf(bytes,"%d",&rmm->nv);
      skip = 0;
    } else {
      if (wtflag == 0) sscanf(bytes,"%d %d",&vi,&vj);
      else if (wtflag == 1) sscanf(bytes,"%d %d %d",&vi,&vj,&iwt);
      else sscanf(bytes,"%d %d %lg",&vi,&vj,&dwt);
      edge.vi = vi;
      edge.vj = vj;
      if (wtflag == 0) 
	kv->add((char *) &edge,sizeof(EDGE),NULL,0);
      else if (wtflag == 1) {
	iweight = iwt;
	kv->add((char *) &edge,sizeof(EDGE),(char *) &iweight,sizeof(IWEIGHT));
      } else {
	dweight = dwt;
	kv->add((char *) &edge,sizeof(EDGE),(char *) &dweight,sizeof(DWEIGHT));
      }
    }

    bytes = next + 1;
  }
}
