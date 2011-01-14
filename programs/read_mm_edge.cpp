/* ----------------------------------------------------------------------
   read a Matrix Market edge file with or without edge attributes
   file = file to read
   format of file:
     % comment
     Nv Nv Ne (vertices from 1 to Nv, # of edges listed)
     vertex1 vertex 2 edge-attribute (vertex IDs are from 1 to Nv)
     vertex1 vertex 2 edge-attribute (edge att is missing or int or double)
     ...
   attflag = 0 for ignore edge attributes, 1 for integer, 2 for double
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
#include "mrtype.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReadMMEdge::ReadMMEdge(char *file_in, int attflag_in, MPI_Comm world_in)
{
  file = file_in;
  attflag = attflag_in;
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

  uint64_t nvsum;
  MPI_Allreduce(&nv,&nvsum,1,MRMPI_BIGINT,MPI_SUM,world);
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
  IATTRIBUTE iattribute;
  DATTRIBUTE dattribute;
  char *next;

  ReadMMEdge *rmm = (ReadMMEdge *) ptr;
  int attflag = rmm->attflag;

  int skip = 0;

  while (1) {
    next = strchr(bytes,'\n');
    if (!next) break;

    if (bytes[0] == '%') skip = 1;
    else if (skip) {
      sscanf(bytes,"%d",&rmm->nv);
      skip = 0;
    } else {
      if (attflag == 0) {
	sscanf(bytes,"%ld %ld",&edge.vi,&edge.vj);
	kv->add((char *) &edge,sizeof(EDGE),NULL,0);
      } else if (attflag == 1) {
	  sscanf(bytes,"%ld %ld %d",&edge.vi,&edge.vj,&iattribute);
	  kv->add((char *) &edge,sizeof(EDGE),
		  (char *) &iattribute,sizeof(IATTRIBUTE));
      } else {
	sscanf(bytes,"%ld %ld %lg",&edge.vi,&edge.vj,&dattribute);
	kv->add((char *) &edge,sizeof(EDGE),
		(char *) &dattribute,sizeof(DATTRIBUTE));
      }
    }

    bytes = next + 1;
  }
}
