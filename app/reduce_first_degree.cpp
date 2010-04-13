#include "reduce_first_degree.h"
#include "blockmacros.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceFirstDegree::
ReduceFirstDegree(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal reduce first_degree args");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */
// assign degree of 1st vertex
// emit one KV per edge: ((vi,vj),(degree,0)) or ((vi,vj),(0,degree))
// where vi < vj and degree is assigned to correct vertex

void ReduceFirstDegree::reduce(char *key, int keybytes,
			       char *multivalue, int nvalues, int *valuebytes,
			       KeyValue *kv, void *ptr)
{
  char *value;
  VERTEX vi,vj;
  EDGE edge;
  DEGREE degree;

  vi = *(VERTEX *) key;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  value = multivalue;
  for (int i = 0; i < nvalues; i++) {
    vj = *(VERTEX *) value;
    if (vi < vj) {
      edge.vi = vi;
      edge.vj = vj;
      degree.di = nvalues;
      degree.dj = 0;
      kv->add((char *) &edge,sizeof(EDGE),(char *) &degree,sizeof(DEGREE));
    } else {
      edge.vi = vj;
      edge.vj = vi;
      degree.di = 0;
      degree.dj = nvalues;
      kv->add((char *) &edge,sizeof(EDGE),(char *) &degree,sizeof(DEGREE));
    }
  }

  END_BLOCK_LOOP
}
