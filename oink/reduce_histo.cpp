#include "keyvalue.h"
using namespace MAPREDUCE_NS;

/* ----------------------------------------------------------------------
   histo
   count rows with same # of nonzeroes
   input: one KMV per nonzero count, MV has entry for each row
   output: one KV: key = # of nonzeroes, value = # of rows
------------------------------------------------------------------------- */

void histo(char *key, int keybytes, char *multivalue,
	   int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,(char *) &nvalues,sizeof(int));
}
