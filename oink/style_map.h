#if defined MAP_TASK_STYLE

MapStyle(rmat_generate)

#elif defined MAP_FILE_STYLE

MapStyle(read_edge)
MapStyle(read_edge_label)
MapStyle(read_edge_weight)
MapStyle(read_vertex_label)
MapStyle(read_vertex_weight)
MapStyle(read_words)

#elif defined MAP_STRING_STYLE


#elif defined MAP_MR_STYLE

MapStyle(add_label)
MapStyle(add_weight)
MapStyle(edge_to_vertex)
MapStyle(edge_to_vertex_pair)
MapStyle(edge_to_vertices)
MapStyle(edge_upper)
MapStyle(invert)

#else

#include "mapreduce.h"
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

void rmat_generate(int itask, KeyValue *kv, void *ptr);
void read_edge(int itask, char *file, KeyValue *kv, void *ptr);
void read_edge_label(int itask, char *file, KeyValue *kv, void *ptr);
void read_edge_weight(int itask, char *file, KeyValue *kv, void *ptr);
void read_vertex_label(int itask, char *file, KeyValue *kv, void *ptr);
void read_vertex_weight(int itask, char *file, KeyValue *kv, void *ptr);
void read_words(int itask, char *file, KeyValue *kv, void *ptr);
void add_label(uint64_t itask, char *key, int keybytes, char *value,
	       int valuebytes, KeyValue *kv, void *ptr);
void add_weight(uint64_t itask, char *key, int keybytes, char *value,
		int valuebytes, KeyValue *kv, void *ptr);
void edge_to_vertex(uint64_t itask, char *key, int keybytes, char *value,
		    int valuebytes, KeyValue *kv, void *ptr);
void edge_to_vertex_pair(uint64_t itask, char *key, int keybytes, char *value,
			 int valuebytes, KeyValue *kv, void *ptr);
void edge_to_vertices(uint64_t itask, char *key, int keybytes, char *value,
		      int valuebytes, KeyValue *kv, void *ptr);
void edge_upper(uint64_t itask, char *key, int keybytes, char *value,
		int valuebytes, KeyValue *kv, void *ptr);
void invert(uint64_t itask, char *key, int keybytes, char *value,
	    int valuebytes, KeyValue *kv, void *ptr);

#endif
