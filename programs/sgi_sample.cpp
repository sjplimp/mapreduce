/* ----------------------------------------------------------------------
   sample sub-graph isomorphic matches in a graph
   msample = 0 = just count
   msample > 0 = count and then randomly sample M of them
   ntour = # of vertices N in tour of sub-graph to match
   vtour = attributes of N vertices in tour
   ftour = identity flag of N vertices in tour
   etour = attributes of N-1 edges in tour 
   input MR:
     mrv = one KV per vertex = Vi : Wi
     mre = one KV per edge = Eij : Fij
     mrs,mrx,mry = workspace
   output MR:
     mrv,mre are unchanged
     mrs = one KV per isomorphic match = Vn : V1 V2 ... Vn-1
     nsgi = # of matches
   datatypes: Vi = uint64, Wi and Fij = int
 ------------------------------------------------------------------------- */

#include "mpi.h"
#include "math.h"
#include "stdint.h"
#include "stdlib.h"
#include "string.h"
#include "blockmacros.h"
#include "sgi_sample.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

SGISample::SGISample(int msample_in, int ntour_in,
		     int *vtour_in, int *ftour_in, int *etour_in,
		     MPI_Comm world_in)
{
  msample = msample_in;
  ntour = ntour_in;
  vtour = vtour_in;
  ftour = ftour_in;
  etour = etour_in;
  world = world_in;

  MPI_Comm_rank(world,&me);
}

/* ---------------------------------------------------------------------- */

double SGISample::run(MapReduce *mrv, MapReduce *mre,
		      MapReduce **mro_in, MapReduce **mri_in,
		      MapReduce *mreprime_in, MapReduce *mrc_in,
		      uint64_t &nsgi)
{
  MPI_Barrier(world);
  double tstart = MPI_Wtime();

  mro = mro_in;
  mri = mri_in;
  mreprime = mreprime_in;
  mrc = mrc_in;

  // pre-calculate X = Eij : Wi Wj Fij, one for each edge in big graph

  mrv->aggregate(NULL);
  mrc->map(mre,map1,this);
  mrc->aggregate(NULL);
  mrc->add(mrv);
  mrc->convert();
  mrc->reduce(reduce1a,this);
  mrc->aggregate(NULL);
  mrc->add(mrv);
  mrc->convert();
  mrc->reduce(reduce1b,this);

  // debug
  //mrc->map(mrc,x3print,this,1);

  // create outbound and inbound BPG (bipartite graph) layers
  // create mro and mri lists of MapReduce objects

  for (int k = 0; k < ntour-1; k++) mro[k]->open();
  for (int k = 1; k < ntour; k++) mri[k]->open();
  
  mrc->map(mrc,map2,this);

  for (int k = 0; k < ntour-1; k++) {
    mro[k]->close();
    mro[k]->aggregate(NULL);
  }
  for (int k = 1; k < ntour; k++) {
    mri[k]->close();
    mri[k]->aggregate(NULL);
  }

  // debug
  //for (int k = 0; k < ntour-1; k++) {
  // printf("OOO MR outbound %d\n",k);
  // mro[k]->map(mro[k],bpgprint,this,1);
  //}
  //for (int k = 1; k < ntour; k++) {
  //  printf("III MR inbound %d\n",k);
  //  mri[k]->map(mri[k],bpgprint,this,1);
  // }

  // downward sweep thru BPG using outbound edges
  // accumulate count on each edge in mreprime

  mrc->map(mro[0],map3,NULL);
  mrc->convert();
  mrc->reduce(reduce3,this);

  // debug
  //mrc->map(mrc,r3print,this,1);
  //mrc->verbosity = 1;

  mreprime->open();

  int n;

  eflag = 0;
  for (int k = 0; k < ntour-1; k++) {
    if (me == 0) printf("Down Sweep %d\n",k);
    mrc->add(mro[k]);
    mrc->convert();
    kindex = k;
    mrc->reduce(reduce4a,this);
    mrc->collate(NULL);
    mrc->reduce(reduce4b,this);
    n = mrc->aggregate(NULL);
  }

  // first calc of T

  tlocal = 0;
  mrc->map(mrc,map5,this);
  MPI_Allreduce(&tlocal,&nsgi,1,MPI_UNSIGNED_LONG,MPI_SUM,world);
  if (me == 0) printf("PATHS ON DOWN SWEEP = %lu\n",nsgi);

  // upward sweep thru BPG using inbound edges
  // accumulate 2nd count on each edge in mreprime

  mrc->map(mri[ntour-1],map3,this);
  mrc->convert();
  mrc->reduce(reduce3,this);

  eflag = 1;
  for (int k = ntour-1; k > 0; k--) {
    if (me == 0) printf("Up Sweep %d\n",k);
    mrc->add(mri[k]);
    mrc->convert();
    kindex = k-1;
    mrc->reduce(reduce4a,this);
    mrc->collate(NULL);
    mrc->reduce(reduce4b,this);
    mrc->aggregate(NULL);
  }

  mreprime->close();

  // second calc of T

  tlocal = 0;
  mrc->map(mrc,map5,this);
  MPI_Allreduce(&tlocal,&nsgi,1,MPI_UNSIGNED_LONG,MPI_SUM,world);
  if (me == 0) printf("PATHS ON UP SWEEP = %lu\n",nsgi);

  /*
  // calculate Sk

  mreprime->collate(NULL);
  for (int k = 0; k < ntour; k++) mrsk[k]->open();
  mreprime->reduce(reduce5,this);
  for (int k = 0; k < ntour; k++) mrsk[k]->collate(NULL);
  for (int k = 0; k < ntour; k++) mrsk[k]->reduce(reduce6,NULL);

  // enumerate all walks thru BPG

  if (msample >= nsgi) {

    mrp->open();
    mrsk[0]->map(map7,this);

    for (int k = 0; k < ntour; k++) {
      mrp->add(mrsk[k]);
      mrp->convert();
      mrp->reduce(reduce7,this);
    }

  // sample M walks thru BPG

  } else {
  }

  */

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  return tstop-tstart;
}

/* ---------------------------------------------------------------------- */

void SGISample::map1(uint64_t itask, char *key, int keybytes, 
		     char *value, int valuebytes, KeyValue *kv, void *ptr) 
{
  X1VALUE newvalue;

  EDGE *edge = (EDGE *) key;
  newvalue.vj = edge->vj;
  newvalue.fij = *(LABEL *) value;
  kv->add((char *) &edge->vi,sizeof(VERTEX),(char *) &newvalue,sizeof(X1VALUE));
}

/* ---------------------------------------------------------------------- */

void SGISample::map2(uint64_t itask, char *key, int keybytes, 
		     char *value, int valuebytes, KeyValue *kv, void *ptr) 
{
  SGISample *sgi = (SGISample *) ptr;
  MapReduce **mro = sgi->mro;
  MapReduce **mri = sgi->mri;
  int ntourm1 = sgi->ntour - 1;
  int *vtour = sgi->vtour;
  int *etour = sgi->etour;

  EDGE *edge = (EDGE *) key;
  X3VALUE *x = (X3VALUE *) value;
  
  for (int k = 0; k < ntourm1; k++) {
    if (x->wi == vtour[k] && x->fij == etour[k] && x->wj == vtour[k+1]) {
      mro[k]->kv->add((char *) &edge->vi,sizeof(VERTEX),
		      (char *) &edge->vj,sizeof(VERTEX));
      mri[k+1]->kv->add((char *) &edge->vj,sizeof(VERTEX),
			(char *) &edge->vi,sizeof(VERTEX));
    }
    if (x->wj == vtour[k] && x->fij == etour[k] && x->wi == vtour[k+1]) {
      mro[k]->kv->add((char *) &edge->vj,sizeof(VERTEX),
		      (char *) &edge->vi,sizeof(VERTEX));
      mri[k+1]->kv->add((char *) &edge->vi,sizeof(VERTEX),
			(char *) &edge->vj,sizeof(VERTEX));
    }
  }
}

/* ---------------------------------------------------------------------- */

void SGISample::map3(uint64_t itask, char *key, int keybytes, 
		     char *value, int valuebytes, KeyValue *kv, void *ptr) 
{
  kv->add(key,keybytes,NULL,0);
}

/* ---------------------------------------------------------------------- */

void SGISample::map5(uint64_t itask, char *key, int keybytes, 
		     char *value, int valuebytes, KeyValue *kv, void *ptr) 
{
  SGISample *sgi = (SGISample *) ptr;
  ULONG *tlocal = &sgi->tlocal;
  ULONG tone = *(ULONG *) value;
  *tlocal += tone;
}

/* ---------------------------------------------------------------------- */

void SGISample::reduce1a(char *key, int keybytes, 
			 char *multivalue, int nvalues, int *valuebytes,
			 KeyValue *kv, void *ptr) 
{
  VERTEX vj;
  X1VALUE *oldvalue;
  X2VALUE newvalue;
  int i;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)

  // find Wi

  LABEL wi;
  int flag = 0;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  for (i = 0; i < nvalues; i++)
    if (valuebytes[i] == sizeof(LABEL)) {
      wi = *(LABEL *) &multivalue[i*sizeof(X1VALUE)];
      flag = 1;
      break;
    }

  if (flag) break;

  END_BLOCK_LOOP

  // emit all edges flipped

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  int offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(LABEL)) {
      oldvalue = (X1VALUE *) &multivalue[offset];
      vj = oldvalue->vj;
      newvalue.vi = *(VERTEX *) key;
      newvalue.wi = wi;
      newvalue.fij = oldvalue->fij;
      kv->add((char *) &vj,sizeof(VERTEX),(char *) &newvalue,sizeof(X2VALUE));
    }
    offset += valuebytes[i];
  }

  END_BLOCK_LOOP
}

/* ---------------------------------------------------------------------- */

void SGISample::reduce1b(char *key, int keybytes, 
			 char *multivalue, int nvalues, int *valuebytes,
			 KeyValue *kv, void *ptr) 
{
  VERTEX vi,vj;
  EDGE edge;
  X2VALUE *oldvalue;
  X3VALUE newvalue;
  int i;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)

  // find Wi

  LABEL wi;
  int flag = 0;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  for (i = 0; i < nvalues; i++)
    if (valuebytes[i] == sizeof(LABEL)) {
      wi = *(LABEL *) &multivalue[i*sizeof(X2VALUE)];
      flag = 1;
      break;
    }

  if (flag) break;

  END_BLOCK_LOOP

  // emit all edges once

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  int offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(LABEL)) {
      oldvalue = (X2VALUE *) &multivalue[offset];
      edge.vi = oldvalue->vi;
      edge.vj = *(VERTEX *) key;
      newvalue.wi = oldvalue->wi;
      newvalue.wj = wi;
      newvalue.fij = oldvalue->fij;
      kv->add((char *) &edge,sizeof(EDGE),
	      (char *) &newvalue,sizeof(X3VALUE));
    }
    offset += valuebytes[i];
  }

  END_BLOCK_LOOP
}

/* ---------------------------------------------------------------------- */

void SGISample::reduce3(char *key, int keybytes, 
			char *multivalue, int nvalues, int *valuebytes,
			KeyValue *kv, void *ptr) 
{
  COUNT count;
  count.count = 1;
  count.dummy = 0;
  kv->add(key,keybytes,(char *) &count,sizeof(COUNT));
}

/* ---------------------------------------------------------------------- */

void SGISample::reduce4a(char *key, int keybytes, 
			 char *multivalue, int nvalues, int *valuebytes,
			 KeyValue *kv, void *ptr) 
{
  ULONG cnt;
  X4VALUE x;
  VERTEX vi,vj;
  int i;

  SGISample *sgi = (SGISample *) ptr;
  MapReduce *mreprime = sgi->mreprime;
  int eflag = sgi->eflag;
  int kindex = sgi->kindex;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)

  // find current count

  int flag = 0;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  for (i = 0; i < nvalues; i++)
    if (valuebytes[i] != sizeof(VERTEX)) {
      COUNT *value = (COUNT *) &multivalue[i*sizeof(VERTEX)];
      cnt = value->count;
      flag = 1;
      break;
    }

  if (flag) break;

  END_BLOCK_LOOP

  // emit one KV for each edge

  vi = *(VERTEX *) key;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  int offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] == sizeof(VERTEX)) {
      vj = *(VERTEX *) &multivalue[offset];
      kv->add((char *) &vj,sizeof(VERTEX),(char *) &cnt,sizeof(ULONG));
      if (eflag == 0) {
	x.vi = vi;
	x.vj = vj;
      } else {
	x.vi = vj;
	x.vj = vi;
      }
      x.k = kindex;
      mreprime->kv->add((char *) &x,sizeof(X4VALUE),
                        (char *) &cnt,sizeof(ULONG));
    }
    offset += valuebytes[i];
  }

  END_BLOCK_LOOP
}

/* ---------------------------------------------------------------------- */

void SGISample::reduce4b(char *key, int keybytes, 
			 char *multivalue, int nvalues, int *valuebytes,
			 KeyValue *kv, void *ptr) 
{
  int i;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)

  // sum counts

  ULONG sum = 0;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  for (i = 0; i < nvalues; i++)
    sum += *(ULONG *) &multivalue[i*sizeof(ULONG)];

  END_BLOCK_LOOP

  // emit new summed count

  COUNT count;
  count.count = sum;
  count.dummy = 0;
  kv->add(key,keybytes,(char *) &count,sizeof(COUNT));
}

/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */

void SGISample::x3print(uint64_t itask, char *key, int keybytes, 
			char *value, int valuebytes, 
			KeyValue *kv, void *ptr) 
{
  EDGE *edge = (EDGE *) key;
  X3VALUE *x = (X3VALUE *) value;
  VERTEX vi = edge->vi;
  VERTEX vj = edge->vj;
  int wi = x->wi;
  int wj = x->wj;
  int fij = x->fij;
  printf("MR X3: %d %d: %lu %lu: %d %d %d\n",
	 keybytes,valuebytes,vi,vj,wi,wj,fij);
}

void SGISample::bpgprint(uint64_t itask, char *key, int keybytes, 
			 char *value, int valuebytes, 
			 KeyValue *kv, void *ptr) 
{
  VERTEX vi = *(VERTEX *) key;
  VERTEX vj = *(VERTEX *) value;
  printf("MR BGP: %d %d: %lu: %lu\n",keybytes,valuebytes,vi,vj);
}

void SGISample::r3print(uint64_t itask, char *key, int keybytes, 
			char *value, int valuebytes, 
			KeyValue *kv, void *ptr) 
{
  VERTEX v = *(VERTEX *) key;
  COUNT *c = (COUNT *) value;
  printf("MR R3: %d %d: %lu: %lu\n",keybytes,valuebytes,v,c->count);
}
