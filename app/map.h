#ifndef MAP_H
#define MAP_H

#include "stdint.h"
#include "pointers.h"

namespace MAPREDUCE_NS {
  class KeyValue;
};

namespace APP_NS {

class Map : protected Pointers {
 public:
  char *id;
  void (*appmap)(int, MAPREDUCE_NS::KeyValue *, void *);
  void (*appmap_file_list)(int, char *, 
			   MAPREDUCE_NS::KeyValue *, void *);
  void (*appmap_file)(int, char *, int,
		      MAPREDUCE_NS::KeyValue *, void *);
  void (*appmap_mr)(uint64_t, char *, int, char *, int, 
		    MAPREDUCE_NS::KeyValue *, void *);
  void *appptr;

  Map(class APP *, char *);
  virtual ~Map();
};

}

#endif
