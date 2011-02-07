#include "keyvalue.h"
using namespace MAPREDUCE_NS;

/* ----------------------------------------------------------------------
   cull
   eliminate duplicate edges
   input: one KMV per edge, MV has multiple entries if duplicates exist
   output: one KV per edge: key = edge, value = NULL
------------------------------------------------------------------------- */

void cull(char *key, int keybytes, char *multivalue,
	  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,NULL,0);
}
