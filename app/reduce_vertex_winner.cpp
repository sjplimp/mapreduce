#include "reduce_vertex_winner.h"
#include "blockmacros.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceVertexWinner::
ReduceVertexWinner(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal reduce vertex_winner command");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */

void ReduceVertexWinner::reduce(char *key, int keybytes,
				char *multivalue, int nvalues, int *valuebytes,
				KeyValue *kv, void *ptr)
{
  int i;
  VFLAG *vf;
  int winflag = 1;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  vf = (VFLAG *) multivalue;
  for (i = 0; i < nvalues; i++) {
    if (vf->flag == 0) {
      winflag = 0;
      break;
    }
    vf++;;
  }
  if (i < nvalues) break;

  END_BLOCK_LOOP

  VERTEX *v = (VERTEX *) key;
  VERTEX v1out,v2out;
  VFLAG vfout;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  vf = (VFLAG *) multivalue;
  for (i = 0; i < nvalues; i++) {
    v1out.v = vf->v;
    v1out.r = vf->r;
    if (winflag) {
      vfout.v = v->v;
      vfout.r = v->r;
      vfout.flag = 0;
      //printf("VWIN EDGE (%u %g) (%u %g %d)\n",
      //	     v1out.v,v1out.r,vfout.v,vfout.r,vfout.flag);
      kv->add((char *) &v1out,sizeof(VERTEX),(char *) &vfout,sizeof(VFLAG));
    } else {
      v2out.v = v->v;
      v2out.r = v->r;
      //printf("VWIN EDGE (%u %g) (%u %g)\n",
      //	     v1out.v,v1out.r,v2out.v,v2out.r);
      kv->add((char *) &v1out,sizeof(VERTEX),(char *) &v2out,sizeof(VERTEX));
    }
    vf++;;
  }

  END_BLOCK_LOOP
}
