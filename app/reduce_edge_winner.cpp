#include "reduce_edge_winner.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

ReduceEdgeWinner::
ReduceEdgeWinner(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg) error->all("Illegal reduce edge_winner command");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */

void ReduceEdgeWinner::reduce(char *key, int keybytes,
			      char *multivalue, int nvalues, int *valuebytes,
			      KeyValue *kv, void *ptr)
{
  EDGE *edge = (EDGE *) key;
  if (valuebytes[0] || (nvalues == 2 && valuebytes[1])) return;

  int winner;
  if (edge->ri < edge->rj) winner = 0;
  else if (edge->rj < edge->ri) winner = 1;
  else if (edge->vi < edge->vj) winner = 0;
  else winner = 1;

  VERTEX v;
  VFLAG vf;
  if (winner == 0) {
    v.v = edge->vi;
    v.r = edge->ri;
    vf.v = edge->vj;
    vf.r = edge->rj;
    vf.flag = 1;
    printf("EWIN EMIT: %u %g %u %g %d\n",v.v,v.r,vf.v,vf.r,vf.flag);
    kv->add((char *) &v,sizeof(VERTEX),(char *)&vf,sizeof(VFLAG));
    v.v = edge->vj;
    v.r = edge->rj;
    vf.v = edge->vi;
    vf.r = edge->ri;
    vf.flag = 0;
    printf("EWIN EMIT: %u %g %u %g %d\n",v.v,v.r,vf.v,vf.r,vf.flag);
    kv->add((char *) &v,sizeof(VERTEX),(char *) &vf,sizeof(VFLAG));
  } else {
    v.v = edge->vj;
    v.r = edge->rj;
    vf.v = edge->vi;
    vf.r = edge->ri;
    vf.flag = 1;
    printf("EWIN EMIT: %u %g %u %g %d\n",v.v,v.r,vf.v,vf.r,vf.flag);
    kv->add((char *) &v,sizeof(VERTEX),(char *) &vf,sizeof(VFLAG));
    v.v = edge->vi;
    v.r = edge->ri;
    vf.v = edge->vj;
    vf.r = edge->rj;
    vf.flag = 0;
    printf("EWIN EMIT: %u %g %u %g %d\n",v.v,v.r,vf.v,vf.r,vf.flag);
    kv->add((char *) &v,sizeof(VERTEX),(char *) &vf,sizeof(VFLAG));
  }
}
