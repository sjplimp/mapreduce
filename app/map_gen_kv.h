#ifdef MAP_CLASS

MapStyle(gen_kv,MapGenKV)

#else

#ifndef MAP_GEN_KV_H
#define MAP_GEN_KV_H

#include "map.h"

namespace APP_NS {

class MapGenKV : public Map {
 public:
  MapGenKV(class APP *, char *, int, char **);
  ~MapGenKV();

 private:
  uint64_t ntotal,nlocal;
  int ktype,vtype;
  int klenlo,klenhi,vlenlo,vlenhi;
  int klo,khi,vlo,vhi;
  char kclo,kchi,vclo,vchi;
  int klenrange,vlenrange;
  int krange,vrange;
  int *kint,*vint;
  char *kstr,*vstr;

  static void map(int, MAPREDUCE_NS::KeyValue *, void *);
};

}

#endif
#endif
