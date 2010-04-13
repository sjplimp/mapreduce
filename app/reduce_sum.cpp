#include "reduce_sum.h"
#include "blockmacros.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceSum::ReduceSum(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal sum args");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */

void ReduceSum::reduce(char *key, int keybytes,
		       char *multivalue, int nvalues, int *valuebytes,
		       KeyValue *kv, void *ptr)
{
  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  nvalues = nvalues_total;
  kv->add(key,keybytes,(char *) &nvalues,sizeof(int));
}
