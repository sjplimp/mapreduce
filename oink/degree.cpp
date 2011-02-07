/* ----------------------------------------------------------------------
   OINK - Mapreduce-MPI library application
   http://www.sandia.gov/~sjplimp/mapreduce.html, Sandia National Laboratories
   Steve Plimpton, sjplimp@sandia.gov

   See the README file in the top-level MR-MPI directory.
------------------------------------------------------------------------- */

#include "mpi.h"
#include "string.h"
#include "stdlib.h"
#include "degree.h"
#include "typedefs.h"
#include "object.h"
#include "error.h"

#include "blockmacros.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "keymultivalue.h"

using namespace OINK_NS;
using namespace MAPREDUCE_NS;

#define MAXLINE 1024

/* ---------------------------------------------------------------------- */

Degree::Degree(OINK *oink) : Command(oink)
{
  ninputs = 1;
  noutputs = 1;
}

/* ---------------------------------------------------------------------- */

void Degree::run()
{
  int me;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);

  // MRe = Eij : NULL

  MapReduce *mre = obj->input(1,read,NULL);
  MapReduce *mrv = obj->create_mr();
  
  uint64_t nedge = mre->kv_stats(0);
  char msg[128];
  sprintf(msg,"Degree: input graph with %lu edges",nedge);
  if (me == 0) error->message(msg);

  // convert edges into vertices and collate them to get counts

  mrv->map(mre,map1,NULL);
  mrv->collate(NULL);
  uint64_t nvert = mrv->reduce(reduce1,NULL);
  mrv->gather(1);
  mrv->sort_values(-1);
   
  obj->output(1,mrv,print,NULL);

  sprintf(msg,"Degree: %lu vertices",nvert);
  if (me == 0) error->message(msg);

  obj->cleanup();
}

/* ---------------------------------------------------------------------- */

void Degree::params(int narg, char **arg)
{
  if (narg != 0) error->all("Illegal degree command");
}

/* ---------------------------------------------------------------------- */

void Degree::read(int itask, char *file, KeyValue *kv, void *ptr)
{
  char line[MAXLINE];
  EDGE edge;

  FILE *fp = fopen(file,"r");
  while (fgets(line,MAXLINE,fp)) {
    sscanf(line,"%lu %lu",&edge.vi,&edge.vj);
    kv->add((char *) &edge,sizeof(EDGE),NULL,0);
  }
  fclose(fp);
}

/* ---------------------------------------------------------------------- */

void Degree::print(char *key, int keybytes, 
		     char *value, int valuebytes, void *ptr) 
{
  FILE *fp = (FILE *) ptr;
  VERTEX vi = *(VERTEX *) key;
  int degree = *(int *) value;
  fprintf(fp,"%lu %d\n",vi,degree);
}

/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */

void Degree::map1(uint64_t itask, char *key, int keybytes, 
		    char *value, int valuebytes, KeyValue *kv, void *ptr) 
{
  EDGE *edge = (EDGE *) key;
  kv->add((char *) &edge->vi,sizeof(VERTEX),(char *) &edge->vj,sizeof(VERTEX));
  kv->add((char *) &edge->vj,sizeof(VERTEX),(char *) &edge->vi,sizeof(VERTEX));
}

/* ---------------------------------------------------------------------- */

void Degree::reduce1(char *key, int keybytes, char *multivalue, int nvalues,
		     int *valuebytes, KeyValue *kv, void *ptr) 
{
  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  int degree = nvalues_total;
  kv->add(key,keybytes,(char *) &degree,sizeof(int));
}
