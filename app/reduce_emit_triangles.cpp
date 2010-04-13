#include "reduce_emit_triangles.h"
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
  if (narg) error->all("Illegal reduce emit_triangles args");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */
// if NULL exists in mvalue, emit other values as triangles
// emit KV as ((vi,vj,vk),None)

void ReduceEmitTriangles::reduce(char *key, int keybytes,
				 char *multivalue, int nvalues,
				 int *valuebytes,
				 KeyValue *kv, void *ptr)
{
  int i;
  for (i = 0; i < nvalues; i++)
    if (valuebytes[i] == 0) break;
  if (i == nvalues) return;

  TRI tri;
  EDGE *edge = (EDGE *) key;
  tri.vj = edge->vi;
  tri.vk = edge->vj;

  int offset = 0;
  for (int i = 0; i < nvalues; i++)
    if (valuebytes[i]) {
      tri.vi = *((VERTEX *) &multivalue[offset]);
      kv->add((char *) &tri,sizeof(TRI),NULL,0);
      offset += valuebytes[i];
    }
}
