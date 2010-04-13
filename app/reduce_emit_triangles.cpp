#include "reduce_emit_triangles.h"
#include "blockmacros.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceEmitTriangles::
ReduceEmitTriangles(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal reduce emit_triangles command");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */

void ReduceEmitTriangles::reduce(char *key, int keybytes,
				 char *multivalue, int nvalues,
				 int *valuebytes,
				 KeyValue *kv, void *ptr)
{
  int i;
  char *value;

  // loop over values to find a NULL

  int flag = 0;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  for (i = 0; i < nvalues; i++)
    if (valuebytes[i] == 0) {
      flag = 1;
      break;
    }
  if (i < nvalues) break;

  END_BLOCK_LOOP

  if (!flag) return;

  // emit triangle for each vertex

  TRI tri;
  EDGE *edge = (EDGE *) key;
  tri.vj = edge->vi;
  tri.vk = edge->vj;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  value = multivalue;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i]) {
      tri.vi = *(VERTEX *) value;
      kv->add((char *) &tri,sizeof(TRI),NULL,0);
    }
    value += valuebytes[i];
  }

  END_BLOCK_LOOP
}
