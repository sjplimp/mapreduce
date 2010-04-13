#include "reduce_cull.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceCull::ReduceCull(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal reduce cull args");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */

void ReduceCull::reduce(char *key, int keybytes,
			char *multivalue, int nvalues, int *valuebytes,
			KeyValue *kv, void *ptr)
{
  kv->add(key,keybytes,NULL,0);
}
