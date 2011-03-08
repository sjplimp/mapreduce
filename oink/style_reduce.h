#ifdef REDUCE_STYLE

ReduceStyle(count)
ReduceStyle(cull)

#else

#include "keyvalue.h"
using namespace MAPREDUCE_NS;

void count(char *key, int keybytes, char *multivalue,
	   int nvalues, int *valuebytes, KeyValue *kv, void *ptr);
void cull(char *key, int keybytes, char *multivalue,
	  int nvalues, int *valuebytes, KeyValue *kv, void *ptr);

#endif
