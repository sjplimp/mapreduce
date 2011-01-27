/* ----------------------------------------------------------------------
   read one or more edge files with or without edge attributes
   format of file: 1st 2 lines are optional
     % comment
     Nv Nv Ne (vertices from 1 to Nv, # of edges listed)
     vertex1 vertex 2 edge-attribute (vertex IDs are from 1 to Nv)
     vertex1 vertex 2 edge-attribute (edge att is missing or int or double)
     ...
   attflag = 0 for ignore edge attributes, 1 for integer, 2 for double
   input MR = empty
   output MR = Eij : wt, Eij = Vi Vj
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

#define MAXLINE 1024

/* ---------------------------------------------------------------------- */

ReadMMEdge::ReadMMEdge(int nstr_in, char **strings_in, int self_in,
		       int attflag_in, MPI_Comm world_in)
{
  nstr = nstr_in;
  strings = strings_in;
  self = self_in;
  attflag = attflag_in;
  world = world_in;
}

/* ---------------------------------------------------------------------- */

double ReadMMEdge::run(MapReduce *mr, uint64_t &nedge)
{
  MPI_Barrier(world);
  double tstart = MPI_Wtime();

  nedge = mr->map(nstr,strings,self,0,0,map,this);

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  return tstop - tstart;
}

/* ---------------------------------------------------------------------- */

void ReadMMEdge::map(int itask, char *file, KeyValue *kv, void *ptr)
{
  char line[MAXLINE];
  EDGE edge;
  IATTRIBUTE iattribute;
  DATTRIBUTE dattribute;

  ReadMMEdge *rmm = (ReadMMEdge *) ptr;
  int attflag = rmm->attflag;

  int skip = 0;

  FILE *fp = fopen(file,"r");
  while (fgets(line,MAXLINE,fp)) {
    if (line[0] == '%') skip = 1;
    else if (skip) skip = 0;
    else {
      if (attflag == 0) {
	sscanf(line,"%lu %lu",&edge.vi,&edge.vj);
	kv->add((char *) &edge,sizeof(EDGE),NULL,0);
      } else if (attflag == 1) {
	  sscanf(line,"%lu %lu %d",&edge.vi,&edge.vj,&iattribute);
	  kv->add((char *) &edge,sizeof(EDGE),
		  (char *) &iattribute,sizeof(IATTRIBUTE));
      } else {
	sscanf(line,"%lu %lu %lg",&edge.vi,&edge.vj,&dattribute);
	kv->add((char *) &edge,sizeof(EDGE),
		(char *) &dattribute,sizeof(DATTRIBUTE));
      }
    }
  }
  fclose(fp);
}
