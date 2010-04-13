#ifndef APP_OBJECT_H
#define APP_OBJECT_H

#include "pointers.h"

namespace MAPREDUCE_NS {
  class MapReduce;
};

namespace APP_NS {

class Object : protected Pointers {
 public:
  Object(class APP *);
  ~Object();

  int classify_object(char *, int &);
  void *find_object(char *, int);
  void create_object(int, int, char **);
  void invoke_object(int, int, char **);
  int variable_object(char *, char *, double &);

 private:
  struct Obj {
    char *id;
    int type;
    void *ptr;
  };

  Obj *objs;
  int nobj,maxobj;

  char **classes;
  int nclass;

  class MRMPI *mrmpi;

  void add_object(char *, int, void *);
  void *add_map(char *, int, char **);
  void *add_reduce(char *, int, char **);
  void *add_hash(char *, int, char **);
  void *add_compare(char *, int, char **);
};

}

#endif
