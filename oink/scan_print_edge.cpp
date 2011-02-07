#include "stdio.h"
#include "typedefs.h"

/* ----------------------------------------------------------------------
   print_edge
   print out an edge
   input: one KMV per edge, MV has multiple entries if duplicates exist
   output: one KV per edge: key = edge, value = NULL
------------------------------------------------------------------------- */

void print_edge(char *key, int keybytes, char *value, int valuebytes, void *ptr) 
{
  FILE *fp = (FILE *) ptr;
  EDGE *edge = (EDGE *) key;
  fprintf(fp,"%lu %lu\n",edge->vi,edge->vj);
}
