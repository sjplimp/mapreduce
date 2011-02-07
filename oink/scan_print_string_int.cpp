#include "stdio.h"

/* ----------------------------------------------------------------------
   print_string_int
   print out string and int
   input: one KMV per edge, MV has multiple entries if duplicates exist
   output: one KV per edge: key = edge, value = NULL
------------------------------------------------------------------------- */

void print_string_int(char *key, int keybytes, 
		      char *value, int valuebytes, void *ptr) 
{
  FILE *fp = (FILE *) ptr;
  int count = *(int *) value;
  fprintf(fp,"%s %d\n",key,count);
}
