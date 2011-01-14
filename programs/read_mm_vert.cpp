/* ----------------------------------------------------------------------
   read a vertex file with vertex attributes
   file = file to read
   format of file:
     % comment
     N (# of vertices listed)
     vertex-ID vertex-attribute (ID is a positive uint64_t)
     vertex-ID vertex-attribute (attribute is int or double)
     ...
   attflag = 1 for integer attributes, 2 for double
   input MR = empty
   aggregate vertices to owning processor
   output MR = Vi : wt
   nvert = # of vertices (from header of file)
   datatypes: Vi = uint64, wt = int or double
 ------------------------------------------------------------------------- */

#include "mpi.h"
#include "string.h"
#include "stdlib.h"
#include "stdint.h"
#include "read_mm_vert.h"
#include "mrtype.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReadMMVert::ReadMMVert(char *file_in, int attflag_in, MPI_Comm world_in)
{
  file = file_in;
  attflag = attflag_in;
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

  uint64_t nvsum;
  MPI_Allreduce(&nv,&nvsum,1,MRMPI_BIGINT,MPI_SUM,world);
  nvert = nvsum;

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  return tstop - tstart;
}

/* ---------------------------------------------------------------------- */

void ReadMMVert::map(int itask, char *bytes, int nbytes, 
		     KeyValue *kv, void *ptr)
{
  VERTEX v;
  IATTRIBUTE iattribute;
  DATTRIBUTE dattribute;
  char *next;

  ReadMMVert *rmm = (ReadMMVert *) ptr;
  int attflag = rmm->attflag;

  int skip = 0;

  while (1) {
    next = strchr(bytes,'\n');
    if (!next) break;

    if (bytes[0] == '%') skip = 1;
    else if (skip) {
      sscanf(bytes,"%ld",&rmm->nv);
      skip = 0;
    } else {
      if (attflag == 1) {
	sscanf(bytes,"%ld %d",&v,&iattribute);
	kv->add((char *) &v,sizeof(VERTEX),
		(char *) &iattribute,sizeof(IATTRIBUTE));
      } else {
	sscanf(bytes,"%ld %lg",&v,&dattribute);
	kv->add((char *) &v,sizeof(VERTEX),
		(char *) &dattribute,sizeof(DATTRIBUTE));
      }
    }

    bytes = next + 1;
  }
}
