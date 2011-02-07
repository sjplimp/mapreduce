#ifdef REDUCE_STYLE

ReduceStyle(cull)
ReduceStyle(degree)
ReduceStyle(histo)
ReduceStyle(nonzero)
ReduceStyle(sum_count)

#else

#include "keyvalue.h"
using namespace MAPREDUCE_NS;

void cull(char *key, int keybytes, char *multivalue,
	  int nvalues, int *valuebytes, KeyValue *kv, void *ptr);
void degree(char *key, int keybytes, char *multivalue,
	 int nvalues, int *valuebytes, KeyValue *kv, void *ptr);
void histo(char *key, int keybytes, char *multivalue,
	   int nvalues, int *valuebytes, KeyValue *kv, void *ptr);
void nonzero(char *key, int keybytes, char *multivalue,
	     int nvalues, int *valuebytes, KeyValue *kv, void *ptr);
void sum_count(char *key, int keybytes, char *multivalue,
	       int nvalues, int *valuebytes, KeyValue *kv, void *ptr);

#endif
