#ifdef REDUCE_STYLE

ReduceStyle(count)
ReduceStyle(cull)

#else

#include "keyvalue.h"
using MAPREDUCE_NS::KeyValue;

void count(char *key, int keybytes, char *multivalue,
	   int nvalues, int *valuebytes, KeyValue *kv, void *ptr);
void cull(char *key, int keybytes, char *multivalue,
	  int nvalues, int *valuebytes, KeyValue *kv, void *ptr);

#endif
