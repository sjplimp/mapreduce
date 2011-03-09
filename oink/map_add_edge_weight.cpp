#include "keyvalue.h"
#include "typedefs.h"
using namespace MAPREDUCE_NS;

/* ----------------------------------------------------------------------
   add_edge_weight
   read edges from file, formatted with 2 vertices per line
   output: key = Vi Vj, value = NULL
------------------------------------------------------------------------- */

void add_edge_weight(uint64_t itask, char *key, int keybytes, char *value,
		     int valuebytes, KeyValue *kv, void *ptr)
{
  WEIGHT one = 1.0;
  kv->add(key,keybytes,(char *) &one,sizeof(WEIGHT));
}
