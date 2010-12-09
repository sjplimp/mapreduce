/* ----------------------------------------------------------------------
   read a vertex file with vertex weights
   file = file to read
   wtflag = 1 for integer weights, 2 for double
   input MR = empty
   aggregate vertices to owning processor
   output MR = Vi : wt
   nvert = # of vertices (from header of file)
   datatypes: Vi = uint64, wt = int or double
 ------------------------------------------------------------------------- */

#include "mpi.h"
#include "string.h"
#include "stdlib.h"
#include "read_mm_vert.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReadMMVert::ReadMMVert(char *file_in, int wtflag_in, MPI_Comm world_in)
{
  file = file_in;
  wtflag = wtflag_in;
  world = world_in;
}

/* ---------------------------------------------------------------------- */

double ReadMMVert::run(MapReduce *mr, uint64_t &nvert)
{
  int nprocs;
  MPI_Comm_size(world,&nprocs);

  MPI_Barrier(world);
  double tstart = MPI_Wtime();

  nv = 0;
  mr->map(nprocs,1,&file,'\n',80,map,this);

  int nvsum;
  MPI_Allreduce(&nv,&nvsum,1,MPI_INT,MPI_SUM,world);
  nvert = nvsum;

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  return tstop - tstart;
}

/* ---------------------------------------------------------------------- */

void ReadMMVert::map(int itask, char *bytes, int nbytes, 
		     KeyValue *kv, void *ptr)
{
  int v,iwt;
  double dwt;
  VERTEX vi;
  IWEIGHT iweight;
  DWEIGHT dweight;
  char *next;

  ReadMMVert *rmm = (ReadMMVert *) ptr;
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
      if (wtflag == 1) sscanf(bytes,"%d %d",&v,&iwt);
      else sscanf(bytes,"%d %lg",&v,&dwt);
      vi = v;
      if (wtflag == 1) {
	iweight = iwt;
	kv->add((char *) &vi,sizeof(VERTEX),(char *) &iweight,sizeof(IWEIGHT));
      } else {
	dweight = dwt;
	kv->add((char *) &vi,sizeof(VERTEX),(char *) &dweight,sizeof(DWEIGHT));
      }
    }

    bytes = next + 1;
  }
}
