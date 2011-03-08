/* ----------------------------------------------------------------------
   OINK - scripting wrapper on MapReduce-MPI library
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
#include "style_map.h"
#include "style_reduce.h"
#include "error.h"

#include "blockmacros.h"
#include "mapreduce.h"
#include "keyvalue.h"

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

  MapReduce *mre = obj->input(1,read_edge,NULL);
  MapReduce *mrv = obj->create_mr();
  uint64_t nedge = mre->kv_stats(0);

  mrv->map(mre,edge_to_vertices,NULL);
  mrv->collate(NULL);
  uint64_t nvert = mrv->reduce(count,NULL);
  mrv->gather(1);
  mrv->sort_values(-1);
   
  obj->output(1,mrv,print,NULL);

  char msg[128];
  sprintf(msg,"Degree: %d vertices, %d edges",nvert,nedge);

  obj->cleanup();
}

/* ---------------------------------------------------------------------- */

void Degree::params(int narg, char **arg)
{
  if (narg != 0) error->all("Illegal degree command");
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
