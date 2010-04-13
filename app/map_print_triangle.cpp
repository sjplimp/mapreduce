#include "mpi.h"
#include "stdio.h"
#include "map_print_triangle.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapPrintTriangle::
MapPrintTriangle(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg != 1) error->all("Illegal map print_triangle command");

  int me;
  MPI_Comm_rank(world,&me);
  char fname[128];
  sprintf(fname,"%s.%d",arg[0],me);
  FILE *fp = fopen(fname,"w");
  if (fp == NULL)
    error->one("Could not open triangle output file");

  appmap_mr = map;
  appptr = fp;
}

/* ---------------------------------------------------------------------- */

void MapPrintTriangle::map(uint64_t itask, char *key, int keybytes, 
			   char *value, int valuebytes, 
			   KeyValue *kv, void *ptr)
{
  FILE *fp = (FILE *) ptr;
  TRI *tri = (TRI *) key;
  fprintf(fp,"%lu %lu %lu\n",tri->vi,tri->vj,tri->vk);
}
