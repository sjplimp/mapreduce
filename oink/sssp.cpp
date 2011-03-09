/* ----------------------------------------------------------------------
   OINK - scripting wrapper on MapReduce-MPI library
   http://www.sandia.gov/~sjplimp/mapreduce.html, Sandia National Laboratories
   Steve Plimpton, sjplimp@sandia.gov

   See the README file in the top-level MR-MPI directory.
------------------------------------------------------------------------- */

#include "typedefs.h"
#include "string.h"
#include "stdlib.h"
#include "sssp.h"
#include "object.h"
#include "style_map.h"
#include "style_reduce.h"
#include "error.h"

#include "blockmacros.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "keymultivalue.h"

using namespace OINK_NS;
using namespace MAPREDUCE_NS;

/* ---------------------------------------------------------------------- */

SSSP::SSSP(OINK *oink) : Command(oink)
{
  ninputs = 1;
  noutputs = 1;
}

/* ---------------------------------------------------------------------- */

void SSSP::run()
{
  int me;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);

  // MRe = Eij : weight

  MapReduce *mre = obj->input(1,read_edge_weight,NULL);
  MapReduce *mrv = obj->create_mr();
  MapReduce *mrdist = obj->create_mr();

  // MRv = list of vertices in the graph
  
  mrv->map(mre,edge_to_vertices,NULL);
  mrv->collate(NULL);
  mrv->reduce(cull,NULL);

  // iterate over SSSP calculations

  for (int iterate = 0; iterate < niterate; iterate++) {



  }

  // MRdist = Vi : distance Vprev

  //obj->output(1,mrdist,print,NULL);

  obj->cleanup();
}

/* ---------------------------------------------------------------------- */

void SSSP::params(int narg, char **arg)
{
  if (narg != 2) error->all("Illegal sssp command");

  niterate = atoi(arg[0]);
  seed = atoi(arg[1]);
}

/* ---------------------------------------------------------------------- */

void SSSP::print(char *key, int keybytes, 
		 char *value, int valuebytes, void *ptr) 
{
  FILE *fp = (FILE *) ptr;
  VERTEX v = *(VERTEX *) key;
  double weight = *(double *) value;
  VERTEX vprev = *(VERTEX *) &value[sizeof(double)];
  fprintf(fp,"%lu %lu %g\n",v,weight,vprev);
}

/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */

// here are sample map/reduce funcs

/*

void SSSP::map_edge_vert(uint64_t itask, char *key, int keybytes, 
			    char *value, int valuebytes, 
			    KeyValue *kv, void *ptr)
{
}

void SSSP::reduce_second_degree(char *key, int keybytes,
				char *multivalue, int nvalues, 
				int *valuebytes, KeyValue *kv, void *ptr)
{
}

*/
