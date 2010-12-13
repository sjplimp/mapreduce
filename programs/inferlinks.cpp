// Inferring links from a list of web accesses.
// Input:   A directed graph of actual web links (links_html).
//          An ordered (wrt time) list of accesses by IPs (url_xforward).
// Output:  A list of inferred links with number of accesses by different
//          individuals greater than some threshold, along with the 
//          number of accesses along the inferred links.
// 

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <float.h>
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.h"
#include "read_fb_data.hpp"
#include "shared.hpp"

using namespace std;
using namespace MAPREDUCE_NS;

/////////////////////////////////////////////////////////////////////////////
template <typename VERTEX, typename EDGE>
class INFER_EDGES {
public:
  INFER_EDGES(MapReduce *mrvert_, MapReduce *mredge_, MapReduce *mrvisit_) :
              mrvert(mrvert_), mredge(mredge_), mrvisit(mrvisit_) 
  {
    MPI_Comm_rank(MPI_COMM_WORLD, &me);
    MPI_Comm_size(MPI_COMM_WORLD, &np); 
  };
  ~INFER_EDGES() {};

  
private:
  MapReduce *mrvert;  // Unique vertices in links_html graph.
  MapReduce *mredge;  // Unique edges (with weights) in links_html graph 
  MapReduce *mrvisit; // Visits (with time indices) from IPs to URLs.
  int me;
  int np;
}

INFER_EDGES::run()
{
  // First, group visits by the IP making the visit.
  // Assume mrvisit has K-V with key = IP, value = {Time index, URL}
  // This assumption may lead to very large multivalues after collate
  // (for IPs that do lots of surfing).  
  // May have to group by IP and time slice (e.g., use both in key).
  mrvisit->collate(NULL);

  // Emit edges between each site visited by the IP in the order visited.
  // Reduce function will have to sort the values by time index.
  // For i=0 to nvalues, emit edge from sorted multivalue[i] to [i+1].
  // Key = edge; value = flag -1 indicating edge is inferred.
  // Check out what sort functionality Steve has in library.
  // out-of-core sort might not be implemented yet; may have to process
  // records in chunks to avoid problems.
  mrvisit->reduce(emit_edges_for_ip);

  // Add links_html edges to the mrvisit edges.
  // Make sure the K-V format matches output from emit_edges_for_ip.
  // Weights should be > 0.
  mrvisit->add(mredge);

  // Collate to collect edges together on processors.
  mrvisit->collate(NULL);

  // Reduce to remove edges that have real instances in links_html graph.
  // Loop through nvalues for looking for positive weights.  
  // If positive, break -- don't emit edge.
  // If no positive weights found, emit edge with nvalues (number of 
  // IPs who inferred this edge).
  // Key = edge; value = nvalues.
  mrvisit->reduce(remove_real_edges);
  
  // Output results; may require a gather to proc 0 and a reduce to write
  // results.  Or create a histogram with number of inferred edges and 
  // number of IPs inferring them.
}


/////////////////////////////////////////////////////////////////////////////
int main(int narg, char **args)
{
  MPI_Init(&narg, &args);
  int me, np;
  MPI_Comm_size(MPI_COMM_WORLD, &np);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);

  if (np < 100) greetings();
#ifdef MRMPI_FPATH
  // Test the file system for writing; some nodes seem to have 
  // trouble writing to local disk.
  test_local_disks();
#endif

  // Create a new MapReduce object, Edges.
  // Map(Edges):  Input graph from files as in link2graph_weighted.
  //              Output:  Key-values representing edges Vi->Vj with weight Wij
  //                       Key = Vi    
  //                       Value = {Vj, Wij} 
  ReadFBData readFB(narg, args, true);
  readURLXFData readURLXF(narg, args);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  MapReduce *mrvert = NULL;  // Vertices in links_html graph.
  MapReduce *mredge = NULL;  // Edges in links_html graph.  Actual edges.
  uint64_t nverts;    // Number of unique non-zero vertices
  uint64_t nrawedges; // Number of edges in links_html graph.
  uint64_t nedges;    // Number of unique edges in links_html graph.
  readFB.run(&mrvert, &mredge, &nverts, &nrawedges, &nedges);

  MapReduce *mrvisit = NULL;  // Visits (IP, URL) in url_xforward file.
  readURLXF.run(&mrvisit);

  MPI_Barrier(MPI_COMM_WORLD);
  double tmap = MPI_Wtime();

  if (readFB.vertexsize == 16) {
    INFER_EDGES<VERTEX16, EDGE16> infer_edges(mrvert, mredge, mrvisit);
    if (me == 0) cout << "Beginning to infer edges with 16-byte keys." << endl;
    infer_edges.run();
  }
  else if (readFB.vertexsize == 8) {
    INFER_EDGES<VERTEX08, EDGE08> infer_edges(mrvert, mredge, mrvisit);
    if (me == 0) cout << "Beginning to infer edges with 16-byte keys." << endl;
    infer_edges.run();
  }
  else {
    cout << "Invalid vertex size " << readFB.vertexsize << endl;
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  delete mrvert;
  delete mredge;
  delete mrvisit;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  if (me == 0) {
    cout << "Time (Map):         " << tmap - tstart << endl;
    cout << "Time (Infer):       " << tstop - tmap << endl;
    cout << "Time (Total):       " << tstop - tstart << endl;
  }

  MPI_Finalize();
}
