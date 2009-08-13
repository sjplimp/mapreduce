// Single-source shortest paths via MapReduce
// Input:   A directed graph, provided by Karl's files.
// Output:  For each vertex Vi, the shortest weighted distance from a randomly
//          selected source vertex S to Vi, along with the predecessor vertex 
//          of Vi in the shortest weighted path from S to Vi.
// 
// Assume:  Vertices are identified by positive whole numbers in range [1:N].
//          Distances are passed through the MapReduce objects as negative
//          numbers (so we can distinguish them from vertices or weights).
//          Assume edge weights are positive whole numbers.

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.hpp"
#include "read_fb_data.hpp"
#include "renumber_graph.hpp"
#include "shared.hpp"

using namespace std;
using namespace MAPREDUCE_NS;

typedef void REDUCE_FN(char *, int, char *, int, int *, KeyValue *, void *);
REDUCE_FN bfs_with_distances;
REDUCE_FN last_distance_update;
REDUCE_FN default_vtx_distance;
REDUCE_FN output_distances;

void add_source(int, KeyValue *, void *);

/////////////////////////////////////////////////////////////////////////////
int main(int narg, char **args)
{
  MPI_Init(&narg, &args);
  int me, np;
  MPI_Comm_size(MPI_COMM_WORLD, &np);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);

  if (np < 100) greetings();

  // Get input options.
  int nexp = 10;    // Number of experiments to run.
  
  // Create a new MapReduce object, Edges.
  // Map(Edges):  Input graph from files as in link2graph_weighted.
  //              Output:  Key-values representing edges Vi->Vj with weight Wij
  //                       Key = Vi    
  //                       Value = {Vj, Wij} 
  ReadFBData readFB(narg, args);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  MapReduce *mrvert = NULL;
  MapReduce *mredge = NULL;
  int nverts;    // Number of unique non-zero vertices
  int nrawedges; // Number of edges in input files.
  int nedges;    // Number of unique edges in input files.
  readFB.run(&mrvert, &mredge, &nverts, &nrawedges, &nedges);

  // update mrvert and mredge so their vertices are unique ints from 1-N,
  // not hash values
  renumber_graph(readFB.vertexsize, mrvert, mredge);

  srand48(1l);
nexp = 1; // KDDKDD

  for (int exp = 0; exp < nexp; exp++) {
    // Create a new MapReduce object, Paths.
    // Select a source vertex.  
    //       Processor 0 selects random number S in range [1:N] for N vertices.
    //       Processor 0 emits into Paths key-value pair [S, {-1, 0}], 
    //       signifying that vertex S has distance zero from itself, with no
    //       predecessor.

    MapReduce *mrpath = new MapReduce(MPI_COMM_WORLD);
    VERTEX source = -1;
    if (me == 0) {
      source = drand48() * nverts + 1;
source = 6; // KDDKDD
      printf("Source vertex:  %d\n", source);
      mrpath->map(1, add_source, &source);
    }
    MPI_Bcast(&source, 1, MPI_INT, 0, MPI_COMM_WORLD);
printf("KDDKDD after add_source:  %d KVs\n", mrpath->kv->nkey);

    //  Perform a BFS from S, editing distances as visit vertices.
    int done = 0;
    while (!done) {
      done = 1;
   
      // Add Edges to Paths.
      // Collate Paths by vertex; collects edges and any distances 
      // computed so far.
#ifdef NEW_OUT_OF_CORE
      mrpath->add(mredge);
#else
      mrpath->kv->add(mredge->kv);
#endif
printf("KDDKDD after add(mredge):  %d KVs\n", mrpath->kv->nkey);

      mrpath->collate(NULL);
      mrpath->reduce(bfs_with_distances, &done);
printf("KDDKDD after bfs_with_distances:  %d KVs\n", mrpath->kv->nkey);

      int alldone;
      MPI_Allreduce(&done, &alldone, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
      done = alldone;
    }

    // Finish up:  Paths may have more than one distance per vertex.  Take the 
    //             best.
    mrpath->collate(NULL);
    mrpath->reduce(last_distance_update, NULL);

printf("KDDKDD after last_distance_update:  %d KVs\n", mrpath->kv->nkey);

    // Output results:  Include vertices that are not on a path from S.
#ifdef NEW_OUT_OF_CORE
    MapReduce *mrinit = mrvert->copy();
#else
    MapReduce *mrinit = new MapReduce(*mrvert);
#endif
    mrinit->clone();
    mrinit->reduce(default_vtx_distance, NULL);

#ifdef NEW_OUT_OF_CORE
    mrpath->add(mrinit);
#else
    mrpath->kv->add(mrinit->kv);
#endif
printf("KDDKDD after add(mrinit):  %d KVs\n", mrpath->kv->nkey);

    mrpath->collate(NULL);

    char filename[32];
    sprintf(filename, "distance_from_%d.%d", source, np);
    FILE *fp = fopen(filename, "w");

    mrpath->reduce(output_distances, (void *) fp);
printf("KDDKDD after output_distances:  %d KVs\n", mrpath->kv->nkey);

    fclose(fp);
   
    delete mrinit;
    delete mrpath;
  } 
}

/////////////////////////////////////////////////////////////////////////////
// add_source:  Add the source vertex to the MapReduce object as initial vtx.
// Map:    Input:   randomly selected vertex in [1:N] for source.
//         Output:  One key-value pair for the source.
void add_source(int nmap, KeyValue *kv, void *ptr)
{
  VERTEX *v = (VERTEX *) ptr;
  EDGE e;
  e.v = -1;  // No predecessor on path from source to itself.
  e.wt = 0;  // Distance from source to itself is zero.
  kv->add((char *) v, sizeof(VERTEX), (char *) &e, sizeof(EDGE));
}

/////////////////////////////////////////////////////////////////////////////
// bfs_with_distances:  Do breadth-first search, keeping track of shortest
// distance from source.
// Reduce:  Input:   Key-multivalue 
//                   Key = Vi
//                   Multivalue = [{Vj, Wij} for all adj vertices Vj] + 
//                                 (possibly) {Vk, -Dk} representing
//                                 shortest distance from S to Vi through
//                                 preceding vertex Vk.
//                 
//          Compute: If any distances from S to Vi have been computed so far, 
//                   find minimum distance D; keep track of the preceding
//                   vertex Vd giving this best distance.  
//                   If changed the minimum distance, done = 0.
//
//          Output:  Only if a minimum distance was computed, emit one key-value
//                   for each adjacent vertex Vj:
//                   Key = Vj
//                   Value = {Vi, -(D+Wij)}
//                   Also emit best distance so far for Vi:
//                   Key = Vi
//                   Value = {Vd, -D}, where Vd is the preceding vertex
//                           corresponding to the best distance.
void bfs_with_distances(char *key, int keybytes, char *multivalue,
                        int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  int *done = (int *) ptr;
  VERTEX vi = *((VERTEX *) key);

  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)


  // First, find the shortest distance to Vi, if any have been computed yet.
  bool found = false;
  EDGE shortest;            // The shortest path so far to Vi.
  shortest.wt = -INT_MAX;   // Distances are represented as negative wts.
  shortest.v = -1;

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  EDGE *e = (EDGE *) multivalue;
  for (int j = 0; j < nvalues; j++) {
printf("KDDKDD BFS nvalues %d e[%d].wt = %d\n", nvalues, j, e[j].wt);
    if (e[j].wt <= 0) {  // This is a distance value.
      found = true;
      if (e[j].wt > shortest.wt) {  // e[j].wt is the shortest path so far.
        if (shortest.wt != -INT_MAX) *done = 0;  // Changing the weights.
        shortest.wt = e[j].wt;
        shortest.v = e[j].v;
      }
    }
  }

  END_BLOCK_LOOP

  if (found) {
    // Emit best distance so far for Vi.
    kv->add(key, keybytes, (char *) &shortest, sizeof(EDGE));
  
    // Next, augment the path from Vi to each Vj with the weight Wij.
    BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)
  
    EDGE *e = (EDGE *) multivalue;
    for (int j = 0; j < nvalues; j++) {
      if (e[j].wt > 0) {  // This is an adjacency value.
        EDGE dist;
        dist.v = vi;    // Predecessor of Vj along the path.
        dist.wt = shortest.wt - e[j].wt; // Distances are stored as neg. wts.
        kv->add((char *) &(e[j].v), sizeof(VERTEX),
                (char *) &dist, sizeof(EDGE));
      }
    }
  
    END_BLOCK_LOOP
  }
}

/////////////////////////////////////////////////////////////////////////////
//  Reduce:  Input:   Key-multivalue
//                    Key = Vi
//                    Multivalue = [{Vk, -Dk}] representing the shortest
//                                 distance from S to Vi through preceding
//                                 vertex Vk.
//
//           Compute: Find minimum distance D, keeping track of corresponding
//                    preceding vertex Vd.
//
//           Output:  Emit distance from S to Vi:
//                    Key = Vi
//                    Value = {Vd, D}
void last_distance_update(char *key, int keybytes, char *multivalue,
                          int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{

  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)

  // First, find the shortest distance to Vi, if any have been computed yet.
  EDGE shortest;            // The shortest path so far to Vi.
  shortest.wt = -INT_MAX;   // Distances are represented as negative wts.
  shortest.v = -1;

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  EDGE *e = (EDGE *) multivalue;
  for (int j = 0; j < nvalues; j++) {
    if (e[j].wt <= 0) {  // This is a distance value.
      if (e[j].wt > shortest.wt) {  // e->wt is the shortest path so far.
        shortest.wt = e[j].wt;
        shortest.v = e[j].v;
      }
    }
  }

  END_BLOCK_LOOP

  // Then emit the best distance from S to Vi.
  kv->add(key, keybytes, (char *) &shortest, sizeof(EDGE));
}

/////////////////////////////////////////////////////////////////////////////
//  Reduce:  Input:   Key-multivalue
//                    Key = Vi hashkey
//                    Multivalue = Vi in [1:N]
//
//           Output:  Key = Vi
//                    Value = default shortest distance INT_MAX through vtx -1.
void default_vtx_distance(char *key, int keybytes, char *multivalue,
                          int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  EDGE shortest;
  shortest.v = -1;
  shortest.wt = -INT_MAX;

  VERTEX v = *((VERTEX *) multivalue);
  kv->add((char *) &v, sizeof(VERTEX), (char *) &shortest, sizeof(EDGE));
}

/////////////////////////////////////////////////////////////////////////////
//  Reduce:  Input:   Key-multivalue
//                    Key = Vi
//                    Multivalue = [{Vk, -Dk}] representing the shortest
//                                 distance from S to Vi through preceding
//                                 vertex Vk.
//                    Note that nvalues should equal one or two.
//                    If nvalues == 1, Vi is not connected to S.
//                    If nvalues == 2, report the shorter distance.
//
//           Compute: Find minimum distance D, keeping track of corresponding
//                    preceding vertex Vd.
//
//           Output:  Write path entries to a file
//                    Vi D Vd
//                    No key-values emitted.
void output_distances(char *key, int keybytes, char *multivalue,
                      int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  FILE *fp = (FILE *) ptr;
  EDGE *e = (EDGE *) multivalue;

  if (nvalues > 2) {
    printf("Sanity check failed in output_distances:  nvalues = %d\n", nvalues);
    MPI_Abort(MPI_COMM_WORLD,-1);
  }
  
  int shortidx = 0;
  if (nvalues > 1) 
    if (e[1].wt > e[0].wt) shortidx = 1;  // Distances are negative numbers.

  printf("KDD nvalues %d: key %d:  %d  %d\n", nvalues, *((VERTEX *)key), -e[0].wt, (nvalues > 1 ? -e[1].wt : -234));
  fprintf(fp, "%d %d %d\n", *((VERTEX *)key), -e[shortidx].wt, e[shortidx].v);
}
