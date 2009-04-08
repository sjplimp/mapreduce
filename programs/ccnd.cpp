// Connected Components via MapReduce
// Karen Devine and Steve Plimpton, Sandia Natl Labs
// Nov 2008
//
// Identify connected components in a graph via MapReduce
// algorithm due to Jonathan Cohen.
// The algorithm treats all edges as undirected edges.
// No distances are computed.

#include "mpi.h"
#include "math.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "random_mars.h"
#include "assert.h"
#include "limits.h"

#include "test_cc_common.h"

using namespace std;
using namespace MAPREDUCE_NS;


static void reduce1(char *, int, char *, int, int *, KeyValue *, void *);
static void reduce2(char *, int, char *, int, int *, KeyValue *, void *);
static void reduce3(char *, int, char *, int, int *, KeyValue *, void *);
static void reduce4(char *, int, char *, int, int *, KeyValue *, void *);
static void reduce2a(char *, int, char *, int, int *, KeyValue *, void *);
static void reduce3a(char *, int, char *, int, int *, KeyValue *, void *);
static void reduce3b(char *, int, char *, int, int *, KeyValue *, void *);
static int reduce2a_hash(char *, int);
static int reduce3a_hash(char *, int);

/* ---------------------------------------------------------------------- */

typedef struct {
  EDGE e;       
  ZONE zone;
} REDUCE2VALUE;

// Some synonyms just to keep straight what's what in reduce functions.
#define REDUCE3VALUE  EDGEZONE
#define REDUCE3AVALUE EDGEZONE
#define REDUCE3BVALUE EDGEZONE

typedef struct {
  ZONE zone;
  int row;
} REDUCE2AKEY;

typedef struct {
  EDGE e;       
  ZONE zone;
  int col;
} REDUCE2AVALUE;

typedef struct {
  ZONE zone;
  int col;
} REDUCE3AKEY;

/* ---------------------------------------------------------------------- */

void ConnectedComponentsNoDistances(
  MapReduce *mr,      // MapReduce object
  CC *cc,             // Control structure for connected-component calculation.
  int *numSingletons  // Number of singleton vertices (vertices with no edges).
)
{
// Return the labeling of connected components in a mapreduce object.
// Assume the MapReduce object has already been filled by a map with
// key-value pairs; this fill is done by reading files or generating
// an input.  key = Vi; value = Edge incident on Vi.
// At end of routine, the MapReduce object contains KV = vertices with 
// updated state:   key = Vi, value = (Eij,Zi)

  int me = cc->me;
  int twophase = cc->twophase;

  int nVtx = mr->collate(NULL);
  int nCC;
  *numSingletons = cc->nvtx - nVtx;  // Num vertices with degree zero.

  mr->reduce(&reduce1,&cc);

  int iter = 0;

  while (1) {

    mr->collate(NULL);

    if (twophase) {
      mr->reduce(&reduce2a,cc);
      nCC = mr->collate(reduce2a_hash);   //  Want to hash on row here.
    }
    else {
      mr->reduce(&reduce2,cc);
      nCC = mr->collate(NULL);
    }

    iter++;
    if (me == 0) printf("Iteration %d Number of Components = %d\n", iter, nCC);

    cc->doneflag = 1;
    if (twophase) {
      mr->reduce(&reduce3a,cc);
      nCC = mr->collate(reduce3a_hash);   //  Want to hash on col here.
      mr->reduce(&reduce3b,cc);
    }
    else  // !twophase
      mr->reduce(&reduce3,cc);

    int alldone;
    MPI_Allreduce(&cc->doneflag,&alldone,1,MPI_INT,MPI_MIN,MPI_COMM_WORLD);
    if (alldone) break;

    mr->collate(NULL);

    mr->reduce(&reduce4,cc);
  }

  if (me == 0) printf("Number of iterations = %d\n", iter);
}

////////////////////////////////////////////////////////////////////////////
// SHARED REDUCE FUNCTIONS.
////////////////////////////////////////////////////////////////////////////
/* ----------------------------------------------------------------------
   reduce1 function
   Input:  One KMV per vertex; MV lists all edges incident to the vertex.
   Output:  One KV per edge: key = edge e_ij; value = initial state_i
   Initial state of a vertex k is zone=k.
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE1(v, e, s) \
    printf("reduce1:  Vertex %d  Key EDGE (%d %d) Value ZONE %d\n", \
            v, e->vi, e->vj, s);  
#define HELLO_REDUCE1(v, n) \
    printf("HELLO REDUCE1 Vertex %d Nvalues %d\n", *v, nvalues);
#else
#define PRINT_REDUCE1(v, e, s)
#define HELLO_REDUCE1(v, n)
#endif

static void reduce1(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  VERTEX *v = (VERTEX *) key;
  EDGE *e = (EDGE *) multivalue;
  ZONE zone;

  HELLO_REDUCE1(v, nvalues);

  zone = *v;
  for (int n = 0; n < nvalues; n++, e++) {
    kv->add((char *) e, sizeof(EDGE), (char *) &zone, sizeof(ZONE));
    PRINT_REDUCE1(*v, e, zone);
  }
}

/* ----------------------------------------------------------------------
   reduce4 function
   Input:  One KMV per vertex; MV is (e_ij, state_i) for all edges incident
           to v_i.
   Output:  One KV for each edge incident to v_i, with updated state_i.
           key = e_ij; value = new state_i
------------------------------------------------------------------------- */

#ifdef NOISY
#define PRINT_REDUCE4(v, e, s) \
    printf("reduce4:  Vertex %d  Key (%d %d) Value ZONE %d \n", \
            v, e.vi, e.vj, s);  
#define HELLO_REDUCE4(key, nvalues) \
    printf("HELLO REDUCE4 Vertex %d Nvalues %d\n", *((VERTEX *)key), nvalues);
#else
#define PRINT_REDUCE4(v, e, s)
#define HELLO_REDUCE4(key, nvalues)
#endif

static void reduce4(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  HELLO_REDUCE4(key, nvalues);

  // Compute best state for this vertex.
  // Best state has min zone.
  REDUCE3VALUE *r = (REDUCE3VALUE *) multivalue;
  ZONE best;
  int n;

  best = r->zone;
  r++;  // Processed 0th entry already.  Move on.
  for (n = 1; n < nvalues; n++, r++) 
    if (r->zone < best) 
      best = r->zone;

  // Emit edges with updated state for vertex key.
  for (n = 0, r = (REDUCE3VALUE *) multivalue; n < nvalues; n++, r++) {
    kv->add((char *) &(r->e), sizeof(EDGE), (char *) &best, sizeof(ZONE));
    PRINT_REDUCE4(*((VERTEX *) key), r->e, best);
  }
}


////////////////////////////////////////////////////////////////////////////
// ONE-PHASE REDUCE3 REDUCE FUNCTIONS.
////////////////////////////////////////////////////////////////////////////
/* ----------------------------------------------------------------------
   reduce2 function
   Input:  One KMV per edge; MV lists state_i, state_j of v_i, v_j in edge e_ij.
   Output: KV pair for each zone of the edge.
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE2(key, rout) \
    printf("reduce2:  Key %d Value [Edge (%d %d) Zone %d]\n", \
           *key, rout.e.vi, rout.e.vj, \
           rout.zone);  
#define HELLO_REDUCE2(key, nvalues) \
   printf("HELLO REDUCE2  (%d %d) nvalues %d\n", \
          ((EDGE *)key)->vi, ((EDGE *)key)->vj, nvalues);
#else
#define PRINT_REDUCE2(key, rout) 
#define HELLO_REDUCE2(key, nvalues) 
#endif


static void reduce2(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  HELLO_REDUCE2(key, nvalues);

  assert(nvalues >= 2);  // For graphs, each edge has two vertices, so 
                         // the multivalue should have at least two states.
                         // Since we allow reduce4 to emit duplicates,
                         // reduce2 may receive more than 2 states.

  ZONE *si = (ZONE *) multivalue; 
  ZONE *sj = si+1;

  // We are allowing duplicate edges to be emitted by reduce4.
  // If this edge is completely within one zone, all multivalues will be the 
  // same.  But just in case the edge isn't completely within one zone, we have
  // to check duplicates for different zones.  Once we have two different
  // zones or have searched all duplicates, we can move on.
  if (*si == *sj)
    for (int i = 2; i < nvalues; i++) {
      sj = si + i;
      if (*si != *sj) break;
    }
  
  REDUCE2VALUE rout;

  rout.e = *((EDGE *) key);

  // Pick the better zone for this edge's vertices; better zone has lower value.
  ZONE better = *si;
  if (*sj < *si) 
    better = *sj;
  rout.zone = better;

  if (*si == *sj) {
    kv->add((char *) si, sizeof(ZONE), (char *) &rout, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(si, rout);
  }
  else {
    kv->add((char *) si, sizeof(ZONE), (char *) &rout, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(si, rout);

    kv->add((char *) sj, sizeof(ZONE), (char *) &rout, sizeof(REDUCE2VALUE));
    PRINT_REDUCE2(sj, rout);
  }
}

/* ----------------------------------------------------------------------
   reduce3 function
   input KMV = all edges in zone
   one value in multi-value = Eij, Zi
     Eij = (Vi,Vj)
   output KV = vertices with updated state
     key = Vi, value = (Eij,Zi)
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE3(key, value) \
    printf("reduce3:  Key %d Value [Edge (%d %d) Zone %d]\n", \
           key, value.e.vi, value.e.vj, value.zone)
#define HELLO_REDUCE3(key, nvalues) \
   printf("HELLO REDUCE3  %d  nvalues %d\n", key,  nvalues)
#else
#define PRINT_REDUCE3(key, value) 
#define HELLO_REDUCE3(key, nvalues) 
#endif


static void reduce3(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  CC *cc = (CC *) ptr;
  int i;
  ZONE thiszone = *((ZONE *) key);

  HELLO_REDUCE3(thiszone, nvalues);
  
  // Find smallest zone among all vertices in edges in this zone.
  REDUCE2VALUE *value;
  int minzone = INT_MAX;
  for (i = 0, value = (REDUCE2VALUE*)multivalue; i < nvalues; i++, value++) {
    if (value->zone < minzone) minzone = value->zone;
  }

  if (thiszone != minzone) cc->doneflag = 0;

  // Relabel all vertices in zone to have minzone.
  REDUCE3VALUE value3;
  value3.zone = minzone;

  for (i = 0, value = (REDUCE2VALUE*)multivalue; i < nvalues; i++, value++) {

    VERTEX vi = value->e.vi;
    VERTEX vj = value->e.vj;
    value3.e = value->e;
    kv->add((char *) &vi,sizeof(VERTEX), 
            (char *) &value3,sizeof(REDUCE3VALUE));
    PRINT_REDUCE3(vi, value3);

    kv->add((char *) &vj,sizeof(VERTEX), 
            (char *) &value3,sizeof(REDUCE3VALUE));
    PRINT_REDUCE3(vj, value3);
  }
}


////////////////////////////////////////////////////////////////////////////
// TWO-PHASE REDUCE3 REDUCE FUNCTIONS.
////////////////////////////////////////////////////////////////////////////

/* ----------------------------------------------------------------------
   reduce2a function
   Input:  One KMV per edge; MV lists state_i, state_j of v_i, v_j in edge e_ij.
   Output: KV pair for each zone of the edge.
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE2A(key, rval) \
    printf("reduce2:  Key (%d %d) Value [Edge (%d %d) Zone %d Col %d]\n", \
           key.zone, key.row, rval.e.vi, rval.e.vj, \
           rval.zone, rval.col);  
#define HELLO_REDUCE2A(key, nvalues) \
   printf("HELLO REDUCE2  (%d %d) nvalues %d\n", \
          ((EDGE *)key)->vi, ((EDGE *)key)->vj, nvalues);
#else
#define PRINT_REDUCE2A(key, rval) 
#define HELLO_REDUCE2A(key, nvalues) 
#endif


static void reduce2a(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  HELLO_REDUCE2A(key, nvalues);
  CC *cc = (CC *) ptr;

  assert(nvalues >= 2);  // For graphs, each edge has two vertices, so 
                         // the multivalue should have at least two states.
                         // Since we allow reduce4 to emit duplicates,
                         // reduce2 may receive more than 2 states.

  ZONE *si = (ZONE *) multivalue; 
  ZONE *sj = si+1;

  // We are allowing duplicate edges to be emitted by reduce4.
  // If this edge is completely within one zone, all multivalues will be the 
  // same.  But just in case the edge isn't completely within one zone, we have
  // to check duplicates for different zones.  Once we have two different
  // zones or have searched all duplicates, we can move on.
  if (*si == *sj)
    for (int i = 2; i < nvalues; i++) {
      sj = si + i;
      if (*si != *sj) break;
    }
  
  REDUCE2AVALUE rval;
  REDUCE2AKEY rkey;
  RanMars *random = cc->random;
  rkey.row = (int) (random->uniform() * cc->nprocs);

  rval.e = *((EDGE *) key);
  rval.col = (int) (random->uniform() * cc->nprocs);

  // Pick the better zone for this edge's vertices; better zone has lower value.
  ZONE better = *si;
  if (*sj < *si) 
    better = *sj;
  rval.zone = better;

  if (*si == *sj) {
    rkey.zone = *si;
    kv->add((char *) &rkey, sizeof(REDUCE2AKEY), 
            (char *) &rval, sizeof(REDUCE2AVALUE));
    PRINT_REDUCE2A(rkey, rval);
  }
  else {
    rkey.zone = *si;
    kv->add((char *) &rkey, sizeof(REDUCE2AKEY), 
            (char *) &rval, sizeof(REDUCE2AVALUE));
    PRINT_REDUCE2A(rkey, rval);

    rkey.zone = *sj;
    kv->add((char *) &rkey, sizeof(REDUCE2AKEY), 
            (char *) &rval, sizeof(REDUCE2AVALUE));
    PRINT_REDUCE2A(rkey, rval);
  }
}

/* ----------------------------------------------------------------------
   reduce3a function
   input one KMV pair per (zone, row) containing edges in (zone, row)
   one value in multi-value = Eij, Zone_i, Col_i
     Eij = (Vi,Vj)
   output KV = edges from (zone, row) with updated best zone
     key = (zone, col), value = (Eij,Z_best_in_row)
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE3A(key, value) \
    printf("reduce3a:  Key (%d %d) Value [Edge (%d %d) Zone %d]\n", \
           key.zone, key.col, value.e.vi, value.e.vj, value.zone)
#define HELLO_REDUCE3A(key, nvalues) \
   printf("HELLO REDUCE3A  (%d %d) nvalues %d\n", key->zone, key->row,  nvalues)
#else
#define PRINT_REDUCE3A(key, value) 
#define HELLO_REDUCE3A(key, nvalues) 
#endif


static void reduce3a(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  CC *cc = (CC *) ptr;
  int i;
  REDUCE2AKEY *rkey = (REDUCE2AKEY *) key;

  HELLO_REDUCE3A(rkey, nvalues);
  
  // Find smallest zone among all vertices in edges in this (zone.
  REDUCE2AVALUE *value;
  int minzone = IBIGVAL;
  for (i = 0, value = (REDUCE2AVALUE*)multivalue; i < nvalues; i++, value++) {
    if (value->zone < minzone) minzone = value->zone;
  }

  if (rkey->zone != minzone) cc->doneflag = 0;

  // Relabel all vertices in (zone,row) to have minzone.
  REDUCE3AVALUE value3;
  value3.zone = minzone;

  REDUCE3AKEY rkey3;
  rkey3.zone = rkey->zone;

  for (i = 0, value = (REDUCE2AVALUE*)multivalue; i < nvalues; i++, value++) {
    rkey3.col = value->col;
    value3.e = value->e;
    kv->add((char *) &rkey3, sizeof(REDUCE3AKEY), 
            (char *) &value3, sizeof(REDUCE3AVALUE));
    PRINT_REDUCE3A(rkey3, value3);
  }
}

/* ----------------------------------------------------------------------
   reduce3b function
   input one KMV pair per (zone, col) containing edges in (zone, col)
   one value in multi-value = Eij, Zone_best_in_row
     Eij = (Vi,Vj), Zone_best_in_row is best zone from row in reduce3a.
   output KV = vertices with updated state
     key = Vi, value = (Eij,Zi)
------------------------------------------------------------------------- */
#ifdef NOISY
#define PRINT_REDUCE3B(key, value) \
    printf("reduce3b:  Key %d  Value [Edge (%d %d) Zone %d]\n", \
           key, value.e.vi, value.e.vj, value.zone)
#define HELLO_REDUCE3B(key, nvalues) \
   printf("HELLO REDUCE3B  (%d %d) nvalues %d\n", key->zone, key->col,  nvalues)
#else
#define PRINT_REDUCE3B(key, value) 
#define HELLO_REDUCE3B(key, nvalues) 
#endif

static void reduce3b(char *key, int keybytes, char *multivalue,
              int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  CC *cc = (CC *) ptr;
  int i;
  REDUCE3AKEY *rkey = (REDUCE3AKEY *) key;

  HELLO_REDUCE3B(rkey, nvalues);
  
  // Find smallest zone among all vertices in edges in this zone.
  REDUCE3AVALUE *value;
  int minzone = IBIGVAL;
  for (i = 0, value = (REDUCE3AVALUE*)multivalue; i < nvalues; i++, value++) {
    if (value->zone < minzone) minzone = value->zone;
  }

  if (rkey->zone != minzone) cc->doneflag = 0;

  // Relabel all vertices in zone to have minzone.
  REDUCE3BVALUE value3;
  value3.zone = minzone;

  for (i = 0, value = (REDUCE3AVALUE*)multivalue; i < nvalues; i++, value++) {

    VERTEX vi = value->e.vi;
    VERTEX vj = value->e.vj;
    value3.e = value->e;
    kv->add((char *) &vi,sizeof(VERTEX), 
            (char *) &value3,sizeof(REDUCE3BVALUE));
    PRINT_REDUCE3B(vi, value3);

    kv->add((char *) &vj,sizeof(VERTEX), 
            (char *) &value3,sizeof(REDUCE3BVALUE));
    PRINT_REDUCE3B(vj, value3);
  }
}

/* ----------------------------------------------------------------------
   reduce2a_hash function
   Function used to collate keys coming out of reduce2.
   Assign tasks to processor based on their row.
------------------------------------------------------------------------- */

static int reduce2a_hash(char *key, int keybytes)
{
  REDUCE2AKEY *rkey = (REDUCE2AKEY *) key;
  return rkey->row;
}

/* ----------------------------------------------------------------------
   reduce3a_hash function
   Function used to collate keys coming out of reduce3a.
   Assign tasks to processor based on their col.
------------------------------------------------------------------------- */

static int reduce3a_hash(char *key, int keybytes)
{
  REDUCE3AKEY *rkey = (REDUCE3AKEY *) key;
  return rkey->col;
}
