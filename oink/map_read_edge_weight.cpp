#include "typedefs.h"
#include "string.h"
#include "keyvalue.h"
using namespace MAPREDUCE_NS;

#define MAXLINE 1024

/* ----------------------------------------------------------------------
   read_edge_weight
   read edges and weights from file
   file format = 2 vertices and weight per line
   weight = floating point value
   output: key = Vi Vj, value = weight
------------------------------------------------------------------------- */

void read_edge_weight(int itask, char *file, KeyValue *kv, void *ptr)
{
  char line[MAXLINE];
  EDGE edge;
  double weight;

  FILE *fp = fopen(file,"r");
  while (fgets(line,MAXLINE,fp)) {
    sscanf(line,"%lu %lu %g",&edge.vi,&edge.vj,&weight);
    kv->add((char *) &edge,sizeof(EDGE),(char *) &weight,sizeof(double));
  }
  fclose(fp);
}
