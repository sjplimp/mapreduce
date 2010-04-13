#include "reduce_second_degree.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceSecondDegree::
ReduceSecondDegree(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal reduce second_degree args");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */
// assign degree of 2nd vertex
// 2 values per edge, with degree of vi and vj
// emit one KV per edge: ((vi,vj),(deg(i),deg(j))) with vi < vj

void ReduceSecondDegree::reduce(char *key, int keybytes,
				char *multivalue, int nvalues, int *valuebytes,
				KeyValue *kv, void *ptr)
{
  DEGREE *one = (DEGREE *) multivalue;
  DEGREE *two = (DEGREE *) &multivalue[valuebytes[0]];
  DEGREE degree;

  if (one->di) {
    degree.di = one->di;
    degree.dj = two->dj;
    kv->add(key,keybytes,(char *) &degree,sizeof(DEGREE));
  } else {
    degree.di = two->di;
    degree.dj = one->dj;
    kv->add(key,keybytes,(char *) &degree,sizeof(DEGREE));
  }
}
