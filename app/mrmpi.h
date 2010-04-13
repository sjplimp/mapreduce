#ifndef APP_MRMPI_H
#define APP_MRMPI_H

#include "pointers.h"

namespace MAPREDUCE_NS {
  class MapReduce;
};

namespace APP_NS {

class MRMPI : protected Pointers {
 public:
  MRMPI(class APP *);
  ~MRMPI() {}

  void *command(void *, char *, int, char **);
  int variable(void *, char *, double &);

 private:
  MAPREDUCE_NS::MapReduce *mr;

  MAPREDUCE_NS::MapReduce *copy_command(int, char **);
  void add(int, char **);
  void aggregate(int, char **);
  void clone(int, char **);
  void collapse(int, char **);
  void collate(int, char **);
  void compress(int, char **);
  void convert(int, char **);
  void gather(int, char **);
  void map(int, char **);
  void map_file_list(int, char **);
  void map_file_char(int, char **);
  void map_file_str(int, char **);
  void map_mr(int, char **);
  void reduce(int, char **);
  void scrunch(int, char **);
  void sort_keys(int, char **);
  void sort_values(int, char **);
  void sort_multivalues(int, char **);
  void kv_stats(int, char **);
  void kmv_stats(int, char **);
  void cummulative_stats(int, char **);
  void print(int, char **);
  int set(char *, int, char **);
};

}

#endif
