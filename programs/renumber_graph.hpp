// Renumber vertices of in a MapReduce graph so that vertices are in the
// range [1:N] for N vertices.
// Input:  A MapReduce object containing the unique non-zero vertices
//         key = Vi  Value = NULL
//         A MapReduce object containing the unique edges
//         key = Vi  Value = {Vj, Wij} for edge Vi->Vj with weight Wij.
//
// Output:  A MapReduce object containing the unique non-zero vertices
//          renumbered in range [1:N]
//          key = Vi hashkey ID;  Value = Vi in [1:N]
//          A MapReduce object containing the unique edges
//          key = Vi in [1:N]  Value = {Vj in [1:N], Wij} 
//          for edge Vi->Vj with weight Wij.

#ifndef _RENUMBER_GRAPH_HPP
#define _RENUMBER_GRAPH_HPP

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.hpp"
#include "shared.hpp"


using namespace std;
using namespace MAPREDUCE_NS;

void vertex_label(char *, int, char *, int, int *, KeyValue *, void *);
void edge_label1(char *, int, char *, int, int *, KeyValue *, void *);
void edge_label2(char *, int, char *, int, int *, KeyValue *, void *);

void renumber_graph(
  MapReduce *mrvert,   // Input: Unique non-zero vertices with hashkey IDs.
                       // Output:  Unique non-zero vertices with [1:N] numbering
                       // Key = hashkey ID  Value = ID in [1:N].
                       // Note:  overwritten with a new MapReduce object.
  MapReduce *mredge    // Input:  Unique
)
{
// update mredge so its vertices are unique ints from 1-N, not hash values

  int me;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);

  // label.nthresh = # of verts on procs < me

  if (me == 0) printf("Converting hash-keys to integers...\n");
  LABEL label;
  label.count = 0;

#ifdef NEW_OUT_OF_CORE
  int nlocal = mrvert->kv->nkv;
#else
  int nlocal = mrvert->kv->nkey;
#endif

  MPI_Scan(&nlocal,&label.nthresh,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
  label.nthresh -= nlocal;

  mrvert->clone();
  mrvert->reduce(&vertex_label,&label);
    
  // reset all vertices in mredge from 1 to N

#ifdef NEW_OUT_OF_CORE
  mredge->add(mrvert);
#else
  mredge->kv->add(mrvert->kv);
#endif

  mredge->collate(NULL);
  mredge->reduce(&edge_label1,NULL);

#ifdef NEW_OUT_OF_CORE
  mredge->add(mrvert);
#else
  mredge->kv->add(mrvert->kv);
#endif

  mredge->collate(NULL);
  mredge->reduce(&edge_label2,NULL);
}


/* ----------------------------------------------------------------------
   vertex_label reduce() function
   input KMV: (Vi,[])
   output KV: (Vi,ID), where ID is a unique int from 1 to N
------------------------------------------------------------------------- */

void vertex_label(char *key, int keybytes, char *multivalue,
                  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  LABEL *label = (LABEL *) ptr;
  label->count++;
  iVERTEX id;
  id.v = label->nthresh + label->count;
  kv->add(key,keybytes,(char *) &id,sizeof(iVERTEX));
}

/* ----------------------------------------------------------------------
   edge_label1 reduce() function
   input KMV: (Vi,[{Vj,Wj} {Vk,Wk} ...]), one of the mvalues is a 1-N int ID
   output KV: (Vj,{-IDi,Wj}) (Vk,{-IDi,Wk}) ...
------------------------------------------------------------------------- */

void edge_label1(char *key, int keybytes, char *multivalue,
                 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{


  // Identify id = int ID of vertex key in mvalue list.
  iVERTEX id;
  int i, offset, found=0;

  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] == sizeof(iVERTEX)) break;
    offset += valuebytes[i];
  }
  if (i < nvalues) {
    id.v = - (*((iVERTEX *) &multivalue[offset])).v;
    found = 1;
    BREAK_BLOCK_LOOP;
  }

  END_BLOCK_LOOP


  // Sanity check
  if (!found) {
    printf("Error in edge_label1; id not found.\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  // Now relabel vertex key using the ID found and emit reverse edges Vj->key.
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(iVERTEX)) {
      // For key, assuming v is first field of both EDGE16 and EDGE8.
      uint64_t *newkey = (uint64_t *)(&multivalue[offset]);
      iEDGE val;
      val.v = id;
      if (keybytes == 16)
        val.wt = (*((EDGE16 *)(&multivalue[offset]))).wt;
      else
        val.wt = (*((EDGE08 *)(&multivalue[offset]))).wt;
      kv->add((char*)newkey,keybytes,(char*)&val,sizeof(iEDGE));
    }
    offset += valuebytes[i];
  }

  END_BLOCK_LOOP
}

/* ----------------------------------------------------------------------
   edge_label2 reduce() function
   input KMV: (Vi,[{-IDj,Wi} {-IDk,Wi} ...]+one of the mvalues is a positive 
   int = IDi.
   Note that the edges are backward on input.  
   And Wi can differ for each IDj.
   output KV: (IDj,{IDi,Wi}) (IDk,{IDi,Wi}) ...
------------------------------------------------------------------------- */

void edge_label2(char *key, int keybytes, char *multivalue,
                 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{

  // id = positive int in mvalue list

  int i;
  int offset;
  iVERTEX id;

  // Identify id = int ID of vertex key in mvalue list.
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] == sizeof(iVERTEX)) break;
    offset += valuebytes[i];
  }
  if (i < nvalues) {
    id = *((iVERTEX *) &multivalue[offset]);
    BREAK_BLOCK_LOOP;
  }

  END_BLOCK_LOOP

  // Now relabel vertex key using the ID found and emit edges key->Vj using IDs.
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  offset = 0;
  for (i = 0; i < nvalues; i++) {
    if (valuebytes[i] != sizeof(iVERTEX)) {
      iEDGE *mv = (iEDGE *)&(multivalue[offset]);
      iVERTEX vi;
      vi.v = -((mv->v).v);
      iEDGE tmp;
      tmp.v = id;
      tmp.wt = mv->wt;
      kv->add((char *) &vi,sizeof(iVERTEX),(char *) &tmp,sizeof(iEDGE));
    }
    offset += valuebytes[i];
  }

  END_BLOCK_LOOP
}

#endif
