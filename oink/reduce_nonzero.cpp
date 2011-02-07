#include "typedefs.h"
#include "keyvalue.h"
using namespace MAPREDUCE_NS;

/* ----------------------------------------------------------------------
   nonzero
   enumerate nonzeroes in each row
   input: one KMV per edge
   output: one KV per edge: key = row I, value = NULL
------------------------------------------------------------------------- */

void nonzero(char *key, int keybytes, char *multivalue,
	     int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  EDGE *edge = (EDGE *) key;
  kv->add((char *) &edge->vi,sizeof(VERTEX),NULL,0);
}
