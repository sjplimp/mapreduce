#include "reduce_vertex_winner.h"
#include "object.h"
#include "mapreduce.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

enum{MAPREDUCE,MAP,REDUCE,HASH,COMPARE};   // same as in object.cpp

/* ---------------------------------------------------------------------- */

ReduceVertexWinner::
ReduceVertexWinner(APP *app, char *idstr, int narg, char **arg) :
  Reduce(app, idstr)
{
  if (narg != 1) error->all("Illegal reduce vertex_winner command");

  MapReduce *mr = (MapReduce *) obj->find_object(arg[0],MAPREDUCE);
  if (!mr || !mr->kv) error->all("Reduce vertex_winner MR or KV is invalid");
  kv = mr->kv;

  appreduce = reduce;
  appptr = this;
}

/* ---------------------------------------------------------------------- */

void ReduceVertexWinner::reduce(char *key, int keybytes,
				char *multivalue, int nvalues, int *valuebytes,
				KeyValue *kv, void *ptr)
{
  VERTEX *v = (VERTEX *) key;
  VFLAG *vf = (VFLAG *) multivalue;

  int winflag = 1;
  for (int i = 0; i < nvalues; i++) {
    if (vf->flag == 0) {
      winflag = 0;
      break;
    }
    vf++;;
  }

  if (winflag) {
    printf("VWIN VERT %u\n",v->v);
    ReduceVertexWinner *data = (ReduceVertexWinner *) ptr;
    data->kv->add((char *) &v->v,sizeof(uint64_t),NULL,0);
  }

  vf = (VFLAG *) multivalue;
  VERTEX v1out,v2out;
  VFLAG vfout;

  for (int i = 0; i < nvalues; i++) {
    v1out.v = vf->v;
    v1out.r = vf->r;
    if (winflag) {
      vfout.v = v->v;
      vfout.r = v->r;
      vfout.flag = 0;
      printf("VWIN EDGE (%u %g) (%u %g %d)\n",
	     v1out.v,v1out.r,vfout.v,vfout.r,vfout.flag);
      kv->add((char *) &v1out,sizeof(VERTEX),(char *) &vfout,sizeof(VFLAG));
    } else {
      v2out.v = v->v;
      v2out.r = v->r;
      printf("VWIN EDGE (%u %g) (%u %g)\n",
	     v1out.v,v1out.r,v2out.v,v2out.r);
      kv->add((char *) &v1out,sizeof(VERTEX),(char *) &v2out,sizeof(VERTEX));
    }
    vf++;;
  }
}
