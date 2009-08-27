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
#include <iostream>
#include <fstream>
#include <float.h>
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.hpp"
#include "read_fb_data.hpp"
#include "renumber_graph.hpp"
#include "shared.hpp"

using namespace std;
using namespace MAPREDUCE_NS;

/////////////////////////////////////////////////////////////////////////////
// Class used to pass distance information through the MapReduce system.
template <typename VERTEX, typename EDGE>
class DISTANCE {
public:
  DISTANCE(){
    memset(&(e.v.v), 0, sizeof(VERTEX));
    e.wt = FLT_MAX;
    current = false;
  };
  ~DISTANCE(){};
  EDGE e;        // Edge describing the distance of a vtx from S; 
                 // e.v is predecessor vtx; e.wt is distance from S through e.v.
  bool current;  // Flag indicating that this distance is the current state
                 // for the vtx (the currently accepted best distance).
                 // Needed so we can know when to stop (when no vtx distances
                 // change in an iteration).
};


/////////////////////////////////////////////////////////////////////////////
// add_source:  Add the source vertex to the MapReduce object as initial vtx.
// Map:    Input:   randomly selected vertex in [1:N] for source.
//         Output:  One key-value pair for the source.
template <typename VERTEX, typename EDGE>
void add_source(int nmap, KeyValue *kv, void *ptr)
{
  VERTEX *v = (VERTEX *) ptr;
  DISTANCE<VERTEX, EDGE> d;
  d.e.wt = 0;  // Distance from source to itself is zero.
  kv->add((char *) v, sizeof(VERTEX),
          (char *) &d, sizeof(DISTANCE<VERTEX, EDGE>));
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
template <typename VERTEX, typename EDGE>
void bfs_with_distances(char *key, int keybytes, char *multivalue,
                        int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  int *done = (int *) ptr;
  VERTEX *vi = (VERTEX *) key;

  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)


  // First, find the shortest distance to Vi, if any have been computed yet.
  bool found = false;
  DISTANCE<VERTEX, EDGE> previous; // Best distance for Vi from prev iterations.
  DISTANCE<VERTEX, EDGE> shortest; // Shortest path so far to Vi.

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  int offset = 0;
  for (int j = 0; j < nvalues; j++) {
    // Multivalues are either edges or distances.  Distances use more bytes.
    if (valuebytes[j] == sizeof(DISTANCE<VERTEX, EDGE>)) {
      // This is a distance value.
      DISTANCE<VERTEX, EDGE> *d = (DISTANCE<VERTEX, EDGE>*) &multivalue[offset];
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
    kv->add(key, keybytes, (char *) &shortest, sizeof(DISTANCE<VERTEX, EDGE>));

    // Check stopping criterion: not done if (1) this is the first distance
    // computed for Vi, OR (2) the distance for Vi was updated.
    if (!previous.current || 
        (previous.current && (shortest.e.wt != previous.e.wt))) {

      *done = 0;
  
      // Next, augment the path from Vi to each Vj with the weight Wij.
      BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)
  
      int offset = 0;
      for (int j = 0; j < nvalues; j++) {
        if (valuebytes[j] == sizeof(EDGE)) { 
          // This is an adjacency value.
          EDGE *e = (EDGE *) &multivalue[offset];
          DISTANCE<VERTEX, EDGE> dist;
          dist.e.v = *vi;    // Predecessor of Vj along the path.
          dist.e.wt = shortest.e.wt + e->wt; 
          dist.current = false;
          kv->add((char *) &(e->v), sizeof(VERTEX),
                  (char *) &dist, sizeof(DISTANCE<VERTEX, EDGE>));
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
//                    Multivalue = NULL
//
//
//           Output:  Key = Vi
//                    Value = default shortest distance FLT_MAX through vtx -1.
template <typename VERTEX, typename EDGE>
void default_vtx_distance(char *key, int keybytes, char *multivalue,
                          int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  DISTANCE<VERTEX, EDGE> shortest;    // Constructor initializes values.

  kv->add(key, keybytes, (char *) &shortest, sizeof(DISTANCE<VERTEX, EDGE>));
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
template <typename VERTEX, typename EDGE>
void last_distance_update(char *key, int keybytes, char *multivalue,
                          int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)

  // First, find the shortest distance to Vi, if any have been computed yet.
  DISTANCE<VERTEX, EDGE> shortest;     // The shortest path so far to Vi.

  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  DISTANCE<VERTEX, EDGE> *d = (DISTANCE<VERTEX, EDGE> *) multivalue;
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
template <typename VERTEX, typename EDGE>
void output_distances(char *key, int keybytes, char *multivalue,
                      int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  ofstream *fp = (ofstream *) ptr;
//  FILE *fp = (FILE *) ptr;
  EDGE *e = (EDGE *) multivalue;
//  VERTEX *vi = (VERTEX *) key;

  if (nvalues > 1) {
    cout << "Sanity check failed in output_distances:  nvalues = " 
         << nvalues << endl;
    MPI_Abort(MPI_COMM_WORLD,-1);
  }
  
  *fp << *((VERTEX *)key) << "   " << *e << endl;
//  if (keybytes == 16)
//    fprintf(fp, "%lld %lld    %lld %lld  %ld\n",
//            vi->v[0], vi->v[1], edge->v.v[0], edge->v.v[1], edge->wt);
//  else if (keybytes == 8) 
//    fprintf(fp, "%lld    %lld   %ld\n",
//            vi->v[0], edge->v.v[0], edge->wt);
//  else
//    fprintf(fp, "Invalid vertex size %d\n", keybytes);
}


/////////////////////////////////////////////////////////////////////////////
template <typename VERTEX, typename EDGE>
class SSSP {
public:
  SSSP(int narg, char **args, MapReduce *mrvert_, MapReduce *mredge_) :
                                                  mrvert(mrvert_),
                                                  mredge(mredge_),
                                                  tcompute(0.),
                                                  twrite(0.),
                                                  sourcefp(NULL), 
                                                  write_files(false),
                                                  counter(0),
                                                  tnlabeled(0)
  {
    MPI_Comm_rank(MPI_COMM_WORLD, &me); 
    MPI_Comm_size(MPI_COMM_WORLD, &np); 

    // Process input options.  Open the source vertex file on proc 0.
    int iarg = 1;
    while (iarg < narg) {
      if (strcmp(args[iarg], "-s") == 0) {
        iarg++;
        if (me == 0) {
          sourcefp = fopen(args[iarg], "rb");
          if (!sourcefp) {
            cout << "Unable to open source file " << args[iarg] << endl;
            MPI_Abort(MPI_COMM_WORLD, -1);          
          }
        }
      }
      else if (strcmp(args[iarg], "-o") == 0) {
        write_files = true;
      }
      iarg++;
    }
    if (me == 0 && !sourcefp) {
      cout << "Source-vertex file missing; " 
           << "use -s to specify source-vertex file." << endl
           << "(Remember to keep -f or -ff arguments last on command line.)"
           << endl;
      MPI_Abort(MPI_COMM_WORLD, -1);
    }
  };

  ~SSSP()
  {
    if (sourcefp) fclose(sourcefp);
    sourcemap.clear();
  };

  bool run();
  bool get_next_source(VERTEX *);
  double tcompute;  // Compute time
  double twrite;    // Write time
  uint64_t tnlabeled;  // Total number of vtx labeled in all experiments.
private:
  int me;
  int np;
  MapReduce *mrvert;
  MapReduce *mredge;
  FILE *sourcefp;               // Pointer to source-vtx file; set only on 
                                // proc 0.
  map<VERTEX, char> sourcemap;  // unique vertices previouslly used as sources;
                                // populated only on proc 0.
  bool write_files;             // Flag indicating whether to write files
                                // after computing SSSP.
  uint64_t counter;             // Count how many times the SSSP is run().
};

/////////////////////////////////////////////////////////////////////////////
// Routine to read source vertex from a file, determine whether it has been
// used as a source vertex previously, and if not, return it to the application.
// The source file is specified on the command line, with "-s sourcefile".
// It should be in Karl's format.

template <typename VERTEX, typename EDGE>
bool SSSP<VERTEX, EDGE>::get_next_source(
  VERTEX *source
)
{
  source->reset();
  if (me == 0) {
    // Read source vertices from file; keep reading until reach EOF or
    // until find a source vertex that we haven't used before.
    // Keep track of used source vertices in a map (hash table would be better).
    const int RECORDSIZE=32;
    uint64_t buf[4];
    while (1) {
      int nrecords = fread(buf, RECORDSIZE, 1, sourcefp);
      if (nrecords == 0) break;  // EOF; return an invalid source.

      if (buf[0] != 0)  { // Non-zero vertex
        *source = *((VERTEX *) &buf);
        if (sourcemap.find(*source) == sourcemap.end()) {
          // Found a source we haven't used before
          sourcemap[*source] = '1';
          break;  // Stop reading and return this source vertex.
        }
        else
          source->reset();
      }
    }
  }

  MPI_Bcast(source, sizeof(VERTEX), MPI_BYTE, 0, MPI_COMM_WORLD);
  return(source->valid());
}


//////////////////////////////////////////////////////////////////////////////
// Routine to run the SSSP algorithm.
template <typename VERTEX, typename EDGE>
bool SSSP<VERTEX, EDGE>::run() 
{
  // Create a new MapReduce object, Paths.
  // Select a source vertex.  
  //       Processor 0 selects random number S in range [1:N] for N vertices.
  //       Processor 0 emits into Paths key-value pair [S, {-1, 0}], 
  //       signifying that vertex S has distance zero from itself, with no
  //       predecessor.

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  VERTEX source;

  if (!get_next_source(&source))
    return false;  // no unique source remains; quit execution and return.

  MapReduce *mrpath = new MapReduce(MPI_COMM_WORLD);

  mrpath->map(1, add_source<VERTEX,EDGE>, &source);

  //  Perform a BFS from S, editing distances as visit vertices.
  int done = 0;
  int iter = 0;
  uint64_t nlabeled = 0;  // # of vtxs actually labeled during SSSP.
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
    nlabeled = mrpath->reduce(bfs_with_distances<VERTEX,EDGE>, &done);

    int alldone;
    MPI_Allreduce(&done, &alldone, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    done = alldone;
    iter++;
  }

  // Finish up:  Want distance from S to all vertices.  Have to add in 
  //             vertices that are not connected to S through any paths.
#ifdef NEW_OUT_OF_CORE
  MapReduce *mrinit = mrvert->copy();
#else
  MapReduce *mrinit = new MapReduce(*mrvert);
#endif
  mrinit->clone();
  mrinit->reduce(default_vtx_distance<VERTEX,EDGE>, NULL);

#ifdef NEW_OUT_OF_CORE
  mrpath->add(mrinit);
#else
  mrpath->kv->add(mrinit->kv);
#endif
  delete mrinit;

  mrpath->collate(NULL);
  mrpath->reduce(last_distance_update<VERTEX,EDGE>, NULL);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();
  tcompute += (tstop - tstart);

  if (me == 0) cout << counter << ":  Source = " << source
                    << "; Iterations = " << iter 
                    << "; Num Vtx Labeled = " << nlabeled  
                    << "; Compute Time = " << (tstop-tstart) << endl;
  tnlabeled += nlabeled;
  counter++;

  // Now mrpath contains one key-value per vertex Vi:
  // Key = Vi
  // Value = {Vd, D}:  the predecessor vtx Vd, the distance D from 
  //                   S to Vi, and an extraneous flag that we could
  //                   remove.

  // Output results.

  if (write_files) {
    char filename[128];
    if (sizeof(VERTEX) == 16)
      sprintf(filename, "distance_from_%llu_%llu.%03d",
                         source.v[0], source.v[1], me);
    else
      sprintf(filename, "distance_from_%llu.%03d", source.v[0], me);
    ofstream fp;
    fp.open(filename);
  
    mrpath->clone();
    mrpath->reduce(output_distances<VERTEX,EDGE>, (void *) &fp);

    fp.close();
  }

  MPI_Barrier(MPI_COMM_WORLD);
  twrite += (MPI_Wtime() - tstop);
   
  delete mrpath;

  return true;  // Keep going.
}

/////////////////////////////////////////////////////////////////////////////
int main(int narg, char **args)
{
  MPI_Init(&narg, &args);
  int me, np;
  MPI_Comm_size(MPI_COMM_WORLD, &np);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);

  if (np < 100) greetings();

  // Create a new MapReduce object, Edges.
  // Map(Edges):  Input graph from files as in link2graph_weighted.
  //              Output:  Key-values representing edges Vi->Vj with weight Wij
  //                       Key = Vi    
  //                       Value = {Vj, Wij} 
  ReadFBData readFB(narg, args, true);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  MapReduce *mrvert = NULL;
  MapReduce *mredge = NULL;
  uint64_t nverts;    // Number of unique non-zero vertices
  uint64_t nrawedges; // Number of edges in input files.
  uint64_t nedges;    // Number of unique edges in input files.
  readFB.run(&mrvert, &mredge, &nverts, &nrawedges, &nedges);

  MPI_Barrier(MPI_COMM_WORLD);
  double tmap = MPI_Wtime();

  // update mrvert and mredge so their vertices are unique ints from 1-N,
  // not hash values
  // if (me == 0) cout << "Renumbering graph..." << endl;
  // renumber_graph(readFB.vertexsize, mrvert, mredge);

  srand48(1l);
  if (readFB.vertexsize == 16) {
    SSSP<VERTEX16, EDGE16> sssp(narg, args, mrvert, mredge);
    if (me == 0) cout << "Beginning sssp with 16-byte keys." << endl;
    while (sssp.run());
    if (me == 0) {
      cout << "Experiment Time (Compute): " << sssp.tcompute << endl;
      cout << "Experiment Time (Write):   " << sssp.twrite << endl;
      cout << "Total # Vtx Labeled:       " << sssp.tnlabeled << endl;
    }
  }
  else if (readFB.vertexsize == 8) {
    SSSP<VERTEX08, EDGE08> sssp(narg, args, mrvert, mredge);
    if (me == 0) cout << "Beginning sssp with 8-byte keys." << endl;
    while (sssp.run());
    if (me == 0) {
      cout << "Experiment Time (Compute): " << sssp.tcompute << endl;
      cout << "Experiment Time (Write):   " << sssp.twrite << endl;
      cout << "Total # Vtx Labeled:       " << sssp.tnlabeled << endl;
    }
  }
  else {
    cout << "Invalid vertex size " << readFB.vertexsize << endl;
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  delete mrvert;
  delete mredge;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0) {
    cout << "Time (Map):         " << tmap - tstart << endl;
    cout << "Time (Experiments): " << tstop - tmap << endl;
    cout << "Time (Total):       " << tstop - tstart << endl;
  }

  MPI_Finalize();
}
