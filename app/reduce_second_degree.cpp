#include "reduce_second_degree.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceSecondDegree::
ReduceSecondDegree(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal reduce second_degree command");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */

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
