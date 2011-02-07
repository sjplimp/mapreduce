#include "typedefs.h"
#include "keyvalue.h"
using namespace MAPREDUCE_NS;

/* ----------------------------------------------------------------------
   rmat_nonzero
   enumerate row index for each non-zero in RMAT matrix
   output: one KV per edge: key = row I, value = NULL
------------------------------------------------------------------------- */

void rmat_nonzero(uint64_t itask, char *key, int keybytes, char *value,
		  int valuebytes, KeyValue *kv, void *ptr)
{
  EDGE *edge = (EDGE *) key;
  kv->add((char *) &edge->vi,sizeof(VERTEX),NULL,0);
}
