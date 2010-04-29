#include "reduce_vertex_loser.h"
#include "blockmacros.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceVertexLoser::
ReduceVertexLoser(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal reduce vertex_loser command");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */

void ReduceVertexLoser::reduce(char *key, int keybytes,
			       char *multivalue, int nvalues, int *valuebytes,
			       KeyValue *kv, void *ptr)
{
  int i;
  int size = 2*sizeof(uint64_t);
  int loseflag = 0;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] > size) {
      loseflag = 1;
      break;
    }
  }
  if (i < nvalues) break;

  END_BLOCK_LOOP

  VERTEX *v = (VERTEX *) key;
  VFLAG *vf;
  VERTEX v1out,v2out;
  VFLAG vfout;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  for (i = 0; i < nvalues; i++) {
    vf = (VFLAG *) multivalue;
    v1out.v = vf->v;
    v1out.r = vf->r;
    if (loseflag) {
      vfout.v = v->v;
      vfout.r = v->r;
      vfout.flag = 0;
      //printf("VLOSE EDGE (%u %g) (%u %g %d)\n",
      //	     v1out.v,v1out.r,vfout.v,vfout.r,vfout.flag);
      kv->add((char *) &v1out,sizeof(VERTEX),(char *) &vfout,sizeof(VFLAG));
    } else {
      v2out.v = v->v;
      v2out.r = v->r;
      //printf("VLOSE EDGE (%u %g) (%u %g)\n",
      //     v1out.v,v1out.r,v2out.v,v2out.r);
      kv->add((char *) &v1out,sizeof(VERTEX),(char *) &v2out,sizeof(VERTEX));
    }
    multivalue += valuebytes[i];
  }

  END_BLOCK_LOOP
}
