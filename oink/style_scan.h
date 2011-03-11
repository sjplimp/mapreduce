#if defined SCAN_KV_STYLE

ScanStyle(print_edge)
ScanStyle(print_string_int)
ScanStyle(print_vertex)

#elif defined SCAN_KMV_STYLE


#else

void print_edge(char *key, int keybytes,
		char *value, int valuebytes, void *ptr);
void print_string_int(char *key, int keybytes, 
		      char *value, int valuebytes, void *ptr);
void print_vertex(char *key, int keybytes,
		  char *value, int valuebytes, void *ptr);

#endif
