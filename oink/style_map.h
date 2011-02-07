#if defined MAP_TASK_STYLE

MapStyle(rmat_generate)

#elif defined MAP_FILE_STYLE

MapStyle(read_words)

#elif defined MAP_STRING_STYLE


#elif defined MAP_MR_STYLE

MapStyle(rmat_nonzero)
MapStyle(rmat_stats)

#else

#include "keyvalue.h"
using namespace MAPREDUCE_NS;

void rmat_generate(int itask, KeyValue *kv, void *ptr);
void read_words(int itask, char *file, KeyValue *kv, void *ptr);
void rmat_nonzero(uint64_t itask, char *key, int keybytes, char *value,
		  int valuebytes, KeyValue *kv, void *ptr);
void rmat_stats(uint64_t itask, char *key, int keybytes, char *value,
		int valuebytes, KeyValue *kv, void *ptr);

#endif
