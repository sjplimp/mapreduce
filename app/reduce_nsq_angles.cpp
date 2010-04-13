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
  if (narg) error->all("Illegal reduce nsq_angles command");

  appreduce = reduce;
}

/* ---------------------------------------------------------------------- */
// emit Nsq angles associated with each central vertex vi
// emit KV as ((vj,vk),vi) where vj < vk

void ReduceNsqAngles::reduce(char *key, int keybytes,
			     char *multivalue, int nvalues, int *valuebytes,
			     KeyValue *kv, void *ptr)
{
  int j,k,nv,nv2,iblock,jblock;
  char *multivalue2;
  int *valuebytes2;
  VERTEX vj,vk;
  EDGE edge;

  if (nvalues) {
    for (j = 0; j < nvalues-1; j++) {
      vj = *(VERTEX *) &multivalue[j*sizeof(VERTEX)];
      for (k = j+1; k < nvalues; k++) {
	vk = *(VERTEX *) &multivalue[k*sizeof(VERTEX)];
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

  } else {
    MapReduce *mr = (MapReduce *) valuebytes;
    int nblocks;
    mr->multivalue_blocks(nblocks);

    for (iblock = 0; iblock < nblocks; iblock++) { 
      nv = mr->multivalue_block(iblock,&multivalue,&valuebytes);
      for (j = 0; j < nv-1; j++) {
	vj = *(VERTEX *) &multivalue[j*sizeof(VERTEX)];

	for (k = j+1; k < nv; k++) {
	  vk = *(VERTEX *) &multivalue[k*sizeof(VERTEX)];
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

	for (jblock = iblock+1; jblock < nblocks; jblock++) { 
	  nv2 = mr->multivalue_block(jblock,&multivalue2,&valuebytes2);
	  for (k = 0; k < nv2; k++) {
	    vk = *(VERTEX *) &multivalue2[k*sizeof(VERTEX)];
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

	if (iblock < nblocks)
	  mr->multivalue_block(iblock,&multivalue,&valuebytes);
      }
    } 
  }
}
