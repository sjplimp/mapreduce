#if defined SCAN_KV_STYLE

ScanStyle(print_edge)
ScanStyle(print_string_int)

#elif defined SCAN_KMV_STYLE


#else

#include "keyvalue.h"
using namespace MAPREDUCE_NS;

void print_edge(char *key, int keybytes, char *value, int valuebytes, void *ptr);
void print_string_int(char *key, int keybytes, 
		      char *value, int valuebytes, void *ptr);

#endif
