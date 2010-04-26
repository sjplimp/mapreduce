#include "reduce_vertex_loser.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
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
  int size = 2*sizeof(uint64_t);

  int loseflag = 0;
  for (int i = 0; i < nvalues; i++) {
    if (valuebytes[i] > size) {
      loseflag = 1;
      break;
    }
  }

  VERTEX *v = (VERTEX *) key;
  VFLAG *vf;
  EDGE edge;
  int flag = 0;

  for (int i = 0; i < nvalues; i++) {
    vf = (VFLAG *) multivalue;
    if (v->v < vf->v) {
      edge.vi = v->v;
      edge.ri = v->r;
      edge.vj = vf->v;
      edge.rj = vf->r;
    } else {
      edge.vi = vf->v;
      edge.ri = vf->r;
      edge.vj = v->v;
      edge.rj = v->r;
    }
    if (loseflag) {
      printf("VLOSE EDGE (%u %g) (%u %g): %d\n",
	     edge.vi,edge.ri,edge.vj,edge.rj,flag);
      kv->add((char *) &edge,sizeof(EDGE),(char *) &flag,sizeof(int));
    } else {
      printf("VLOSE EDGE (%u %g) (%u %g): NULL\n",
	     edge.vi,edge.ri,edge.vj,edge.rj);
      kv->add((char *) &edge,sizeof(EDGE),NULL,0);
    }
    multivalue += valuebytes[i];
  }

}
