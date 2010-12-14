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
		      MapReduce *mrs, MapReduce *mrx, MapReduce *mry,
		      uint64_t &nsgi)
{
  MPI_Barrier(world);
  double tstart = MPI_Wtime();

  // pre-calculate X = Vi : Vj Wi Wj Fij, two for each edge in big graph

  mrv->aggregate(NULL);
  mrx->map(mre,map1,this);
  mrx->aggregate(NULL);
  mrx->add(mrv);

  // debug
  //mrx->map(mrx,x1print,this,1);

  mrx->convert();
  mrx->reduce(reduce1a,this);
  mrx->aggregate(NULL);
  mrx->add(mrv);

  // debug
  //mrx->map(mrx,x2print,this,1);

  mrx->convert();
  mrx->reduce(reduce1b,this);
  mrx->aggregate(NULL);

  // debug
  //mrx->map(mrx,x3print,this,1);

  // initial S = Vi : NULL

  mrs->map(mrv,map2,this);
  mrs->aggregate(NULL);

  // debug
  //mrs->map(mrs,sprint,this,1);

  // debug

  //mry->verbosity = 2;
  //mrs->verbosity = 2;

  // generate all matches as loop over edges of tour

  for (int k = 1; k < ntour; k++) {
    itour = k-1;
    mry->map(mrx,map3,this);
    mrs->add(mry);
    mrs->convert();

    // debug
    //MapReduce *mrfoo = mrs->copy();
    //mrfoo->reduce(rprint,this);

    nsgi = mrs->reduce(reduce3,this);
    mrs->aggregate(NULL);

    // debug
    //mrs->map(mrs,tprint,this,1);

    if (me == 0) printf("%u sub-graphs after edge %d\n",nsgi,k);
  }

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  // write tours to sgi.out.P

  outfile = "sgi.out";
  fp = NULL;
  mrs->map(mrs,map4,this);
  if (fp) fclose(fp);

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
  int vattsg = sgi->vtour[0];

  LABEL vatt = *(LABEL *) value;
  if (vatt == vattsg) kv->add(key,keybytes,NULL,0);
}

/* ---------------------------------------------------------------------- */

void SGISample::map3(uint64_t itask, char *key, int keybytes, 
		     char *value, int valuebytes, KeyValue *kv, void *ptr) 
{
  SGISample *sgi = (SGISample *) ptr;
  int itour = sgi->itour;
  X3VALUE *x = (X3VALUE *) value;

  if (x->wi == sgi->vtour[itour] && x->fij == sgi->etour[itour] && 
      x->wj == sgi->vtour[itour+1]) {
    if (itour != 1) kv->add(key,keybytes,(char *) &x->vj,sizeof(VERTEX));
    else {
      EDGE pair;
      pair.vi = x->vj;
      pair.vj = 0;
      kv->add(key,keybytes,(char *) &pair,sizeof(EDGE));
    }
  }
}

/* ---------------------------------------------------------------------- */

void SGISample::map4(uint64_t itask, char *key, int keybytes, 
		     char *value, int valuebytes, KeyValue *kv, void *ptr) 
{
  SGISample *sgi = (SGISample *) ptr;

  if (itask == 0) {
    char fname[16];
    sprintf(fname,"%s.%d",sgi->outfile,sgi->me);
    sgi->fp = fopen(fname,"w");
  }
  FILE *fp = sgi->fp;

  int ntourm1 = sgi->ntour - 1;
  VERTEX *vlast = (VERTEX *) key;
  VERTEX *vlist = (VERTEX *) value;

  for (int i = 0; i < ntourm1; i++) fprintf(fp,"%u ",vlist[i]);
  fprintf(fp,"%u\n",*vlast);

  kv->add(key,keybytes,value,valuebytes);
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
  X2VALUE *oldvalue;
  X3VALUE newvalue;
  int i;

  SGISample *sgi = (SGISample *) ptr;
  int ntourm1 = sgi->ntour - 1;
  int itour = sgi->itour;
  int *vtour = sgi->vtour;
  int *etour = sgi->etour;

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

  // emit all edges twice, forward and reverse
  // exclude edges that match nothing in tour

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  int offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(LABEL)) {
      oldvalue = (X2VALUE *) &multivalue[offset];

      vi = *(VERTEX *) key;
      newvalue.vj = oldvalue->vi;
      newvalue.wi = wi;
      newvalue.wj = oldvalue->wi;
      newvalue.fij = oldvalue->fij;
      for (int j = 0; j < ntourm1; j++) {
	if (newvalue.wi == vtour[j] && newvalue.wj == vtour[j+1] && 
	    newvalue.fij == etour[j]) {
	  kv->add((char *) &vi,sizeof(VERTEX),
		  (char *) &newvalue,sizeof(X3VALUE));
	  break;
	}
      }

      vj = oldvalue->vi;
      newvalue.vj = *(VERTEX *) key;
      newvalue.wi = oldvalue->wi;
      newvalue.wj = wi;
      newvalue.fij = oldvalue->fij;
      for (int j = 0; j < ntourm1; j++) {
	if (newvalue.wi == vtour[j] && newvalue.wj == vtour[j+1] && 
	    newvalue.fij == etour[j]) {
	  kv->add((char *) &vj,sizeof(VERTEX),
		  (char *) &newvalue,sizeof(X3VALUE));
	  break;
	}
      }
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
  int i,j,k;

  SGISample *sgi = (SGISample *) ptr;
  int itour = sgi->itour;
  int *vtour = sgi->vtour;
  int *ftour = sgi->ftour;
  int *etour = sgi->etour;
  int toursize = itour*sizeof(VERTEX);
  int newtoursize = toursize + sizeof(VERTEX);
  int vertexsize = sizeof(VERTEX);
  if (itour == 1) vertexsize = sizeof(EDGE);

  //if (!multivalue) {
  //  printf("ERROR: Tour + vertex reduce exceeds one block\n");
  //  MPI_Abort(sgi->world,1);
  //}

  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)

  // ntour = # of values that are current tours
  // nvert = # of values that are new vertices to add to tours

  int ntour = 0;
  int nvert = 0;

  BEGIN_BLOCK_LOOP(multivalue,valuebytes,nvalues)

  for (int i = 0; i < nvalues; i++)
    if (valuebytes[i] == vertexsize) nvert++;
    else ntour++;

  END_BLOCK_LOOP

  if (nvert == 0) return;

  // double loop over tour and vertex values
  // for multiblock, embed double loop in double loop over pairs of blocks
  // for efficiency, put smaller loop on outside
  // emit new tour pairing each current tour with a new vertex
  // exclude an emit based on ftour flag for new vertex

  int flag = ftour[itour+1];
  VERTEX *vlast = (VERTEX *) key;
  VERTEX *tour;
  VERTEX newtour[itour+1];
  VERTEX vi;

  int ioffset,joffset;
  int iblock,jblock,nvalues_i,nvalues_j;
  char *multivalue_i,*multivalue_j;
  int *valuebytes_i,*valuebytes_j;

  if (nvert < ntour) {
    for (iblock = 0; iblock < macro_nblocks; iblock++) {
      if (macro_nblocks > 1) {
	macro_mr->multivalue_block_select(1);
	nvalues_i = 
	  macro_mr->multivalue_block(iblock,&multivalue_i,&valuebytes_i); 
      } else {
	nvalues_i = nvalues;
	multivalue_i = multivalue;
	valuebytes_i = valuebytes;
      }
      for (jblock = 0; jblock < macro_nblocks; jblock++) {
	if (macro_nblocks > 1) {
	  macro_mr->multivalue_block_select(2);
	  nvalues_j = 
	    macro_mr->multivalue_block(jblock,&multivalue_j,&valuebytes_j);
	} else {
	  nvalues_j = nvalues;
	  multivalue_j = multivalue;
	  valuebytes_j = valuebytes;
	}
	
	ioffset = 0;
	for (i = 0; i < nvalues_i; i++) {
	  vi = *(VERTEX *) &multivalue_i[ioffset];
	  ioffset += valuebytes_i[i];
	  if (valuebytes_i[i] != vertexsize) continue;
	  joffset = 0;
	  for (j = 0; j < nvalues_j; j++) {
	    tour = (VERTEX *) &multivalue_j[joffset];
	    joffset += valuebytes_j[j];
	    if (valuebytes_j[j] == vertexsize) continue;
	    if (flag < 0) {
	      for (k = 0; k < itour; k++)
		if (vi == tour[k]) break;
	      if (k < itour) continue;
	    } else if (vi != tour[flag]) continue;
	    memcpy(newtour,tour,toursize);
	    newtour[itour] = *vlast;
	    kv->add((char *) &vi,sizeof(VERTEX),(char *) newtour,newtoursize);
	  }
	}
      }
    }
    
  } else {
    for (iblock = 0; iblock < macro_nblocks; iblock++) {
      if (macro_nblocks > 1) {
	macro_mr->multivalue_block_select(1);
	nvalues_i = 
	  macro_mr->multivalue_block(iblock,&multivalue_i,&valuebytes_i); 
      } else {
	nvalues_i = nvalues;
	multivalue_i = multivalue;
	valuebytes_i = valuebytes;
      }
      for (jblock = 0; jblock < macro_nblocks; jblock++) {
	if (macro_nblocks > 1) {
	  macro_mr->multivalue_block_select(2);
	  nvalues_j = 
	    macro_mr->multivalue_block(jblock,&multivalue_j,&valuebytes_j);
	} else {
	  nvalues_j = nvalues;
	  multivalue_j = multivalue;
	  valuebytes_j = valuebytes;
	}
	
	joffset = 0;
	for (j = 0; j < nvalues_j; j++) {
	  tour = (VERTEX *) &multivalue_j[joffset];
	  joffset += valuebytes_j[j];
	  if (valuebytes_j[j] == vertexsize) continue;
	  memcpy(newtour,tour,toursize);
	  newtour[itour] = *vlast;
	  ioffset = 0;
	  for (i = 0; i < nvalues_i; i++) {
	    vi = *(VERTEX *) &multivalue_i[ioffset];
	    ioffset += valuebytes_i[i];
	    if (valuebytes_i[i] != vertexsize) continue;
	    if (flag < 0) {
	      for (k = 0; k < itour; k++)
		if (vi == tour[k]) break;
	      if (k < itour) continue;
	    } else if (vi != tour[flag]) continue;
	    kv->add((char *) &vi,sizeof(VERTEX),(char *) newtour,newtoursize);
	  }
	}
      }
    }
  }
}
