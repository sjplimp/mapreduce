#include "keyvalue.h"
using namespace MAPREDUCE_NS;

/* ----------------------------------------------------------------------
   degree
   count nonzeroes in each row
   input: one KMV per row, MV has entry for each nonzero
   output: one KV: key = # of nonzeroes, value = NULL
------------------------------------------------------------------------- */

void degree(char *key, int keybytes, char *multivalue,
	 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add((char *) &nvalues,sizeof(int),NULL,0);
}

