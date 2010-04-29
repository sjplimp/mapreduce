/* ----------------------------------------------------------------------
   perform Luby's algorithm to find maximal independent set (MIS) of vertices
   constructor inputs:
     seed = random # seed = positive int
   run() inputs:
     mre = one KV per edge = (Eij,NULL), with all Vi < Vj
     mrv = empty MR to add MIS vertices to
   run() outputs:
     mre is destroyed
     mrv = one KV per MIS vertex = (Vi,NULL)
     niterate = # of iterations required
     nset = # of vertices in MIS
     return elapsed time
 ------------------------------------------------------------------------- */

#include "mpi.h"
#include "math.h"
#include "stdint.h"
#include "stdlib.h"
#include "blockmacros.h"
#include "luby_find.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

LubyFind::LubyFind(int seed_in)
{
  seed = seed_in;
}

/* ---------------------------------------------------------------------- */

double LubyFind::run(MapReduce *mre, MapReduce *mrv, 
		     int &niterate, uint64_t &nset)
{
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // assign a consistent RN to each vertex in each edge, convert to KMV

  mre->map(mre,map_vert_random,&seed);
  mre->clone();

  // loop until all edges deleted

  niterate = 0;
  mrv->open();

  while (1) {
    uint64_t n = mre->reduce(reduce_edge_winner,NULL);
    if (n == 0) break;
    mre->collate(NULL);
    mre->reduce(reduce_vert_winner,NULL);
    mre->collate(NULL);
    mre->reduce(reduce_vert_loser,NULL);
    mre->collate(NULL);
    mre->reduce(reduce_vert_emit,mrv);
    mre->collate(NULL);
    niterate++;
  }

  nset = mrv->close();

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  return tstop-tstart;
}

/* ---------------------------------------------------------------------- */

void LubyFind::map_vert_random(uint64_t itask, char *key, int keybytes, 
			       char *value, int valuebytes, 
			       KeyValue *kv, void *ptr)
{
  VPAIR *vpair = (VPAIR *) key;
  EDGE edge;

  int *seed = (int *) ptr;

  edge.vi = vpair->vi;
  srand48(edge.vi + *seed);
  edge.ri = drand48();
  edge.vj = vpair->vj;
  srand48(edge.vj + *seed);
  edge.rj = drand48();
  kv->add((char *) &edge,sizeof(EDGE),NULL,0);
}

/* ---------------------------------------------------------------------- */

void LubyFind::reduce_edge_winner(char *key, int keybytes,
				  char *multivalue, int nvalues,
				  int *valuebytes, KeyValue *kv, void *ptr)
{
  if (nvalues == 2 && (valuebytes[0] || valuebytes[1])) return;

  EDGE *edge = (EDGE *) key;
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
    kv->add((char *) &v,sizeof(VERTEX),(char *)&vf,sizeof(VFLAG));
    v.v = edge->vj;
    v.r = edge->rj;
    vf.v = edge->vi;
    vf.r = edge->ri;
    vf.flag = 0;
    kv->add((char *) &v,sizeof(VERTEX),(char *) &vf,sizeof(VFLAG));
  } else {
    v.v = edge->vj;
    v.r = edge->rj;
    vf.v = edge->vi;
    vf.r = edge->ri;
    vf.flag = 1;
    kv->add((char *) &v,sizeof(VERTEX),(char *) &vf,sizeof(VFLAG));
    v.v = edge->vi;
    v.r = edge->ri;
    vf.v = edge->vj;
    vf.r = edge->rj;
    vf.flag = 0;
    kv->add((char *) &v,sizeof(VERTEX),(char *) &vf,sizeof(VFLAG));
  }
}

/* ---------------------------------------------------------------------- */

void LubyFind::reduce_vert_winner(char *key, int keybytes,
				  char *multivalue, int nvalues,
				  int *valuebytes, KeyValue *kv, void *ptr)
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
      kv->add((char *) &v1out,sizeof(VERTEX),(char *) &vfout,sizeof(VFLAG));
    } else {
      v2out.v = v->v;
      v2out.r = v->r;
      kv->add((char *) &v1out,sizeof(VERTEX),(char *) &v2out,sizeof(VERTEX));
    }
    vf++;;
  }

  END_BLOCK_LOOP
}

/* ---------------------------------------------------------------------- */

void LubyFind::reduce_vert_loser(char *key, int keybytes,
				 char *multivalue, int nvalues, 
				 int *valuebytes, KeyValue *kv, void *ptr)
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
      kv->add((char *) &v1out,sizeof(VERTEX),(char *) &vfout,sizeof(VFLAG));
    } else {
      v2out.v = v->v;
      v2out.r = v->r;
      kv->add((char *) &v1out,sizeof(VERTEX),(char *) &v2out,sizeof(VERTEX));
    }
    multivalue += valuebytes[i];
  }

  END_BLOCK_LOOP
}

/* ---------------------------------------------------------------------- */

void LubyFind::reduce_vert_emit(char *key, int keybytes,
				char *multivalue, int nvalues, int *valuebytes,
				KeyValue *kv, void *ptr)
{
  int i;
  int size = 2*sizeof(uint64_t);
  int winflag = 1;

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] == size) {
      winflag = 0;
      break;
    }
  }
  if (i < nvalues) break;

  END_BLOCK_LOOP

  VERTEX *v = (VERTEX *) key;
  if (winflag) {
    MapReduce *mrv = (MapReduce *) ptr;
    mrv->kv->add((char *) &v->v,sizeof(uint64_t),NULL,0);
  }

  VFLAG *vf;
  EDGE edge;
  int flag = 0;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  for (i = 0; i < nvalues; i++) {
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
    if (valuebytes[i] == size)
      kv->add((char *) &edge,sizeof(EDGE),NULL,0);
    else
      kv->add((char *) &edge,sizeof(EDGE),(char *) &flag,sizeof(int));
    multivalue += valuebytes[i];
  }

  END_BLOCK_LOOP
}
