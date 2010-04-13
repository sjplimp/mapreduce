#include "reduce_nsq_angles.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceNsqAngles::
ReduceNsqAngles(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal reduce nsq_angles args");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */
// emit Nsq angles associated with each central vertex vi
// emit KV as ((vj,vk),vi) where vj < vk

void ReduceNsqAngles::reduce(char *key, int keybytes,
			     char *multivalue, int nvalues, int *valuebytes,
			     KeyValue *kv, void *ptr)
{
  VERTEX vj,vk;
  EDGE edge;

  for (int j = 0; j < nvalues-1; j++) {
    vj = *((VERTEX *) &multivalue[j*sizeof(VERTEX)]);
    for (int k = j+1; k < nvalues; k++) {
      vk = *((VERTEX *) &multivalue[k*sizeof(VERTEX)]);
      if (vj < vk) {
	edge.vi = vj;
	edge.vj = vk;
	kv->add((char *) &edge,sizeof(EDGE),key,sizeof(VERTEX));
      } else {
	edge.vi = vk;
	edge.vj = vj;
	kv->add((char *) &edge,sizeof(EDGE),key,sizeof(VERTEX));
      }
    }
  }
}
