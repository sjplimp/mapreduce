/* ----------------------------------------------------------------------
   read one or more vertex file with vertex attributes
   format of file(s): 1st 2 lines are optional
     % comment 
     N (# of vertices listed)
     vertex-ID vertex-attribute (ID is a positive uint64_t)
     vertex-ID vertex-attribute (attribute is int or double)
     ...
   attflag = 1 for integer attributes, 2 for double
   input MR = empty
   output MR = Vi : wt
   nvert = # of vertices
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

#define MAXLINE 1024

/* ---------------------------------------------------------------------- */

ReadMMVert::ReadMMVert(int nstr_in, char **strings_in, int self_in,
		       int attflag_in, MPI_Comm world_in)
{
  nstr = nstr_in;
  strings = strings_in;
  self = self_in;
  attflag = attflag_in;
  world = world_in;
}

/* ---------------------------------------------------------------------- */

double ReadMMVert::run(MapReduce *mr, uint64_t &nvert)
{
  MPI_Barrier(world);
  double tstart = MPI_Wtime();

  nvert = mr->map(nstr,strings,self,0,0,map,this);

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  return tstop - tstart;
}

/* ---------------------------------------------------------------------- */

void ReadMMVert::map(int itask, char *file, KeyValue *kv, void *ptr)
{
  char line[MAXLINE];
  VERTEX v;
  IATTRIBUTE iattribute;
  DATTRIBUTE dattribute;
  char *next;

  ReadMMVert *rmm = (ReadMMVert *) ptr;
  int attflag = rmm->attflag;

  int skip = 0;

  FILE *fp = fopen(file,"r");
  while (fgets(line,MAXLINE,fp)) {
    if (line[0] == '%') skip = 1;
    else if (skip) skip = 0;
    else {
      if (attflag == 1) {
	sscanf(line,"%lu %d",&v,&iattribute);
	kv->add((char *) &v,sizeof(VERTEX),
		(char *) &iattribute,sizeof(IATTRIBUTE));
      } else {
	sscanf(line,"%lu %lg",&v,&dattribute);
	kv->add((char *) &v,sizeof(VERTEX),
		(char *) &dattribute,sizeof(DATTRIBUTE));
      }
    }
  }
  fclose(fp);
}
