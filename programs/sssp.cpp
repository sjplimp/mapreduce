// Single-source shortest paths via MapReduce
// Input:   A directed graph, provided by Karl's files.
// Output:  For each vertex Vi, the shortest weighted distance from a randomly
//          selected source vertex S to Vi, along with the predecessor vertex 
//          of Vi in the shortest weighted path from S to Vi.
// 
// Assume:  Vertices are identified by positive whole numbers in range [1:N].
//
// This implementation uses a BFS-like algorithm.  See sssp.txt for details.

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

typedef struct {
  EDGE e;        // Edge describing the distance of a vtx from S; 
                 // e.v is predecessor vtx; e.wt is distance from S through e.v.
  bool current;  // Flag indicating that this distance is the current state
                 // for the vtx (the currently accepted best distance).
                 // Needed so we can know when to stop (when no vtx distances
                 // change in an iteration).
} DISTANCE;

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
      printf("Source vertex:  %d\n", source);
      mrpath->map(1, add_source, &source);
    }
    MPI_Bcast(&source, 1, MPI_INT, 0, MPI_COMM_WORLD);

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

      mrpath->collate(NULL);
      mrpath->reduce(bfs_with_distances, &done);

      int alldone;
      MPI_Allreduce(&done, &alldone, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
      done = alldone;
    }

    // Finish up:  Want distance from S to all vertices.  Have to add in 
    //             vertices that are not connected to S through any paths.
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
    delete mrinit;

    mrpath->collate(NULL);
    mrpath->reduce(last_distance_update, NULL);

    // Now mrpath contains one key-value per vertex Vi:
    // Key = Vi
    // Value = {Vd, D, true}:  the predecessor vtx Vd, the distance D from 
    //                         S to Vi, and an extraneous flag that we could
    //                         remove.

    // Output results.

    char filename[32];
    sprintf(filename, "distance_from_%d.%d", source, np);
    FILE *fp = fopen(filename, "w");

    mrpath->clone();
    mrpath->reduce(output_distances, (void *) fp);

    fclose(fp);
   
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
  DISTANCE d;
  d.e.v = -1;  // No predecessor on path from source to itself.
  d.e.wt = 0;  // Distance from source to itself is zero.
  d.current = false;
  kv->add((char *) v, sizeof(VERTEX), (char *) &d, sizeof(DISTANCE));
}

/////////////////////////////////////////////////////////////////////////////
// bfs_with_distances:  Do breadth-first search, keeping track of shortest
// distance from source.
// Reduce:  Input:   Key-multivalue 
//                   Key = Vi
//                   Multivalue = [{Vj, Wij} for all adj vertices Vj] + 
//                                 (possibly) {Vk, Dk, true/false} representing
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
//                   Value = {Vi, D+Wij, false}
//                   Also emit best distance so far for Vi:
//                   Key = Vi
//                   Value = {Vd, D, true}, where Vd is the preceding vertex
//                           corresponding to the best distance.
void bfs_with_distances(char *key, int keybytes, char *multivalue,
                        int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  int *done = (int *) ptr;
  VERTEX vi = *((VERTEX *) key);

  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)


  // First, find the shortest distance to Vi, if any have been computed yet.
  bool found = false;
  DISTANCE previous;         // Best distance for Vi from previous iterations.
  previous.e.wt = INT_MAX;
  previous.e.v = -1;
  previous.current = false;
  DISTANCE shortest;         // Shortest path so far to Vi.
  shortest.e.wt = INT_MAX; 
  shortest.e.v = -1;

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  int offset = 0;
  for (int j = 0; j < nvalues; j++) {
    // Multivalues are either edges or distances.  Distances use more bytes.
    if (valuebytes[j] == sizeof(DISTANCE)) { // This is a distance value.
      DISTANCE *d = (DISTANCE *) &multivalue[offset];
      found = true;
      if (d->e.wt < shortest.e.wt)  shortest = *d;   // shortest path so far.
      if (d->current) previous = *d;     // currently accepted best distance.
    }
    offset += valuebytes[j];
  }

  END_BLOCK_LOOP

  // if !found, this vtx hasn't been visited along a path from S yet.
  // It is only in mrpath because we added in the entire graph to get the
  // edge lists.  We don't have to emit anything for this vtx.

  if (found) {
    // Emit best distance so far for Vi.
    shortest.current = true;
    kv->add(key, keybytes, (char *) &shortest, sizeof(DISTANCE));

    // Check stopping criterion: not done if (1) this is the first distance
    // computed for Vi, OR (2) the distance for Vi was updated.
    if (!previous.current || 
        (previous.current && (shortest.e.wt != previous.e.wt))) {

      *done = 0;
  
      // Next, augment the path from Vi to each Vj with the weight Wij.
      BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)
  
      int offset = 0;
      for (int j = 0; j < nvalues; j++) {
        if (valuebytes[j] == sizeof(EDGE)) { // This is an adjacency value.
          EDGE *e = (EDGE *) &multivalue[offset];
          DISTANCE dist;
          dist.e.v = vi;    // Predecessor of Vj along the path.
          dist.e.wt = shortest.e.wt + e->wt; 
          dist.current = false;
          kv->add((char *) &(e->v), sizeof(VERTEX),
                  (char *) &dist, sizeof(DISTANCE));
        }
        offset += valuebytes[j];
      }
  
      END_BLOCK_LOOP
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
//  default_vtx_distance:   Earlier, we didn't emit an initial value of 
//  infinity for every vertex, as we'd have to carry that around throughout 
//  the iterations.  But now we'll add initial values in so that we report
//  (infinite) distances for vertices that are not connected to S.
//
//  Reduce:  Input:   Key-multivalue
//                    Key = Vi hashkey
//                    Multivalue = Vi in [1:N]
//
//
//           Output:  Key = Vi
//                    Value = default shortest distance INT_MAX through vtx -1.
void default_vtx_distance(char *key, int keybytes, char *multivalue,
                          int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  DISTANCE shortest;
  shortest.e.wt = INT_MAX;
  shortest.e.v = -1;
  shortest.current = true;

  VERTEX v = *((VERTEX *) multivalue);
  kv->add((char *) &v, sizeof(VERTEX), (char *) &shortest, sizeof(DISTANCE));
}

/////////////////////////////////////////////////////////////////////////////
//  last_distance_update
//  Reduce:  Input:   Key-multivalue
//                    Key = Vi
//                    Multivalue = [{Vk, Dk, true/false}] 
//                                 representing the shortest
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
  DISTANCE shortest;            // The shortest path so far to Vi.
  shortest.e.wt = INT_MAX;  
  shortest.e.v = -1;
  shortest.current = true;

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  DISTANCE *d = (DISTANCE *) multivalue;
  for (int j = 0; j < nvalues; j++)
    if (d[j].e.wt < shortest.e.wt) shortest = d[j]; // shortest path so far.

  END_BLOCK_LOOP

  // Then emit the best distance from S to Vi.
  // Don't need to emit the DISTANCE structure here, as we don't need
  // the stopping-criterion flag any longer.
  kv->add(key, keybytes, (char *) &shortest.e, sizeof(EDGE));
}

/////////////////////////////////////////////////////////////////////////////
//  output_distances: Write the best distance from S to Vi to a file.
//
//  Reduce:  Input:   Key-multivalue
//                    Key = Vi
//                    Multivalue = {Vk, Dk} representing the 
//                                 shortest distance from S to Vi through 
//                                 preceding vertex Vk.
//                    Note that nvalues should equal one.
//
//           Output:  Write path entries to a file
//                    Vi D Vd
//                    No key-values emitted.
void output_distances(char *key, int keybytes, char *multivalue,
                      int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  FILE *fp = (FILE *) ptr;
  EDGE *e = (EDGE *) multivalue;

  if (nvalues > 1) {
    printf("Sanity check failed in output_distances:  nvalues = %d\n", nvalues);
    MPI_Abort(MPI_COMM_WORLD,-1);
  }
  
  fprintf(fp, "%d %d %d\n", *((VERTEX *)key), e->wt, e->v);
}
