#include "reduce_first_degree.h"
#include "blockmacros.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

#define INTMAX 0x7FFFFFFF

/* ---------------------------------------------------------------------- */

ReduceFirstDegree::
ReduceFirstDegree(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal reduce first_degree command");

  appreduce = reduce;
  appptr = this;
}

/* ---------------------------------------------------------------------- */

void ReduceFirstDegree::reduce(char *key, int keybytes,
			       char *multivalue, int nvalues, int *valuebytes,
			       KeyValue *kv, void *ptr)
{
  int i;
  char *value;
  VERTEX vi,vj;
  EDGE edge;
  DEGREE degree;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  if (nvalues_total > INTMAX) {
    ReduceFirstDegree *data = (ReduceFirstDegree *) ptr;
    data->error->one("Too many edges for one vertex in reduce first_degree");
  }
  int ndegree = nvalues_total;

  vi = *(VERTEX *) key;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  value = multivalue;
  for (i = 0; i < nvalues; i++) {
    vj = *(VERTEX *) value;
    if (vi < vj) {
      edge.vi = vi;
      edge.vj = vj;
      degree.di = ndegree;
      degree.dj = 0;
      kv->add((char *) &edge,sizeof(EDGE),(char *) &degree,sizeof(DEGREE));
    } else {
      edge.vi = vj;
      edge.vj = vi;
      degree.di = 0;
      degree.dj = ndegree;
      kv->add((char *) &edge,sizeof(EDGE),(char *) &degree,sizeof(DEGREE));
    }
    value += valuebytes[i];
  }

  END_BLOCK_LOOP
}
