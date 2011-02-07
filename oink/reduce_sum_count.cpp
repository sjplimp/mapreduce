#include "blockmacros.h"
#include "mapreduce.h"
#include "keyvalue.h"
using namespace MAPREDUCE_NS;

/* ----------------------------------------------------------------------
   sum_count
   compute count from nvalues
   input: one KMV per edge, MV has multiple entries if duplicates exist
   output: one KV per edge: key = edge, value = NULL
------------------------------------------------------------------------- */

void sum_count(char *key, int keybytes, char *multivalue,
	       int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  int ntotal = static_cast<int> (nvalues_total);
  kv->add(key,keybytes,(char *) &ntotal,sizeof(int));
}
