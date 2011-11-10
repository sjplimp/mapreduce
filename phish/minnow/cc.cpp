// ************************************************************************
// ** xstream connected components data structures
// ************************************************************************
// ** Author: Jon Berry   (Phillips algorithm)
// ** Description:
// **          This maintains local components of building blocks using
// **          disjoint set structures, a set of spanning tree leaf ids,
// **          and spanning tree edges with dynamic sets.
// **          Trees of purgatories can be created via refinement.  Initial
// **          purgatory resolution is implemented, but the full implementation
// **          of resolution (via "lazy tree mites") remains to be done.
// ** 
// **          This program serves as a beginning for the program that each 
// **          xstream processor will run.  It remains to use Phish to hook
// **          up multiple instances of these.  It also remains to deal with
// **          fire hose movement, the generation of resolution (and all other)
// **          messages, foster edges, and storage.
// **
// ** Usage:  ./a.out < inp.test > out.test
// ** Example inp.test:
// E 1 1 1    2 2 1   1
// P
// E 1 1 1    3 3 1   2
// P
// D 2 3
// P
// E 4 7 3    3 3 1   4
// P
// R 10 10 1  9 7 2   4
// P
// S 4 7 3
// P
//
// Explanation (by line, bb == "building block"):
// add edge:  E <prim_v> <bb_v> <size_bb_v> <prim_w> <bb_w> <size_bb_w> <time>
// print:     P
// delete bb: D <bb_id> <time>
// accept refinement: R <prim_v> <bb_C> <size_bb_C> <prim_w><bb_A><size_A><time>
//                 (refine C from A, with (prim_v, prim_w) as the first cut)
// resolve vertex: S  <prim_v> <destination_bb> <time>
// ************************************************************************
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include <assert.h>
#include <iostream>

#include<list>
#include<set>
#include<map>
#include<queue>

#include "disjoint_sets.hpp"

#define WORK_CHUNK 30        // a constant amount of work per message

#define ACTIVE_REFINEMENT 1
                             // During refinement, we call link_local_components
                             // but forbid either endpoint building block
                             // from becoming a leaf.

#define ALREADY_HAVE_DJSET true 
     // in the case of accepting a refinement, there will already be an
     // entry for a refining node in the DJset structure, and we want to retain
     // that entry.

using namespace std;

bool spanning_tree_leaf(map<int,set<int>*>&, int&);
bool no_spanning_tree_edge(map<int,set<int>*>&, int&);

typedef disjoint_sets<int> DJsets;

static int next_purgatory_id; // negative id's generated on demand
                              // purgatories don't really need to be named
                              // and stored, but we do so for debugging.

struct parcel {
     parcel(int pv, int pw, int bv, int bw, int bvs, int bws, int t) 
          : prim_v(pv), prim_w(pw), bb_v(bv), bb_w(bw), bb_v_size(bvs),
          bb_w_size(bws), time(t) {}
     int prim_v;
     int prim_w;
     int bb_v;
     int bb_w;
     int bb_v_size;
     int bb_w_size;
     int time;
     void print()
     {
          printf("parcel: pv: %d pw: %d bv: %d bw: %d bvs: %d bws: %d t: %d\n",
                 prim_v, prim_w, bb_v, bb_w, bb_v_size, bb_w_size, time);
     }
};

typedef list<parcel> LoadingDock;

typedef map<int, int> Adjacency;
typedef map<int, Adjacency> AdjList;
typedef Adjacency::iterator AdjacencyIterator;
typedef AdjList::iterator AdjListIterator;

class Graph {
public:
     Graph() {}
     bool has_vertex(int v) 
     {
          return (vertices.find(v) != vertices.end());
     }
     void add_vertex(int v) 
     { 
          if (!has_vertex(v)) {
               vertices[v] = Adjacency();
          }
     }
     bool has_edge(int v, int w)
     {
          if (!has_vertex(v) || !has_vertex(w)) {
               return false;
          }
          Adjacency& vadj = vertices[v];
          Adjacency& wadj = vertices[w];
          if ((vadj.find(w) != vadj.end()) &&
              (wadj.find(v) != vadj.end())) {
               return true;
          }
          return false;
     }
     int add_edge(int v, int w, int current_time)
     {
          if (has_edge(v,w)) {
               // update timestamp 
               Adjacency& vadj = vertices[v]; 
               Adjacency& wadj = vertices[w]; 
               int &vtime = vadj[w];
               int &wtime = wadj[v];
               vtime = current_time;
               wtime = current_time;
          } else {
               Adjacency& vadj = vertices[v]; 
               Adjacency& wadj = vertices[w]; 
               vadj[w] = current_time;
               wadj[v] = current_time;
          }
     }
     void remove_edge(int v, int w)
     {
          if (!has_edge(v,w)) {
               return;
          }
          Adjacency& vadj = vertices[v]; 
          Adjacency& wadj = vertices[w]; 
          vadj.erase(w);
          wadj.erase(v);
          // need to track degree and get rid of vertex if degree 0
     }

     void print()
     {
          AdjListIterator adjiter = vertices.begin();
          for ( ; adjiter != vertices.end(); adjiter++) {
               pair<int, Adjacency> p = *adjiter;
               int vertex = p.first;
               printf("%d: ", vertex);
               Adjacency adjs = p.second;
               AdjacencyIterator aiter = adjs.begin();
               for ( ; aiter != adjs.end(); aiter++) {
                    pair<int, int> p = *aiter;
                    int adj = p.first;
                    int timestamp = p.second;
                    printf(" (%d:%d) ", adj, timestamp);
               }
               printf("\n");
          }
     }
     AdjList vertices;
};


list<set<int>* > sets_to_delete;   
set<int> spanning_tree_leaf_pvs;

struct building_block;
set<building_block*> mite_set;     // for resolving purgatories

struct building_block {
     int id;
     int size;
     bool local_component_leader;
     building_block *parent_purgatory;
     building_block *left_child, *right_child; // if I am a purgatory
     int num_to_resolve;   // tree mites will decrement this variable, which
                           // stores the number of vertices that might be
                           // resolved into a descendent of this building block 
                           // from this building block or an ancestory
                           // purgatory.
     int mite_decrement;   // Tree mites will decrement num_to_resolve each
                           // time a vertex is resolves from a purgatory to
                           // a destination node.  The mite_decrement variable
                           // allows us to combine mite operations in order
                           // to guarantee that no more then one mite per
                           // purgatory is required.

     map<int,set<int>* > st_map;
                              // key: near prim_v, value: list of prim_w's, 
                              //      whose current building blocks will be
                              //      looked up in prim_vertex2building block.
                              //      The entries of the latter change during
                              //      purgatory processing, and would leave
                              //      st_map in a bad state if the list stored
                              //      building_block pointers.

     map<int,int> known_vertices;  // (index, last timestamp of outgoing edge)

     building_block(int idd=0, int sz=0) : 
          id(idd), size(sz), local_component_leader(false), parent_purgatory(0),
          left_child(0), right_child(0), num_to_resolve(0), mite_decrement(0)
     {}

     building_block(const building_block& bb) : 
          id(bb.id), size(bb.id),
          local_component_leader(bb.local_component_leader),
          parent_purgatory(bb.parent_purgatory), left_child(bb.left_child),
          right_child(bb.right_child), num_to_resolve(bb.num_to_resolve),
          mite_decrement(bb.mite_decrement)
     {
          printf("error: building block copy constructor called\n");
          exit(1);
     }
     void mite_visit()
     {
          // ***************************************************************
          // 1. In current bb, decrement num_to_resolve by mite_decrement
          // 2. Set mite_decrement to zero
          // 3. Increment mite_decrement for children
          // 4. Create mite_set entries for children
          // 5. Delete mite_set entry for current
          // 6. If num_to_resolve == 0, activation() will get rid of this
          //    building block after this method returns
          // ***************************************************************
          num_to_resolve -= mite_decrement;
          mite_decrement = 0;
          if (left_child && left_child->num_to_resolve > 0) { 
               // left child is an active purgatory
               left_child->mite_decrement++;
               mite_set.insert(left_child);
          }
          if (right_child && right_child->num_to_resolve > 0) { 
               // right child is an active purgatory
               right_child->mite_decrement++;
               mite_set.insert(right_child);
          }
          mite_set.erase(this);
          if (num_to_resolve == 0) { // this purgatory can be deleted.  
               int st_endpoint = 0; 
               if (left_child && left_child->num_to_resolve==0) { 
                    // process non-purgatory left child
                    bool left_is_leaf = spanning_tree_leaf(left_child->st_map, 
                                                           st_endpoint); 
                    if (left_is_leaf) { 
                         spanning_tree_leaf_pvs.insert(st_endpoint); 
                    }
               }
               if (right_child && right_child->num_to_resolve==0) {
                    // process non-purgatory right child
                    bool right_is_leaf = spanning_tree_leaf(right_child->st_map, 
                                                           st_endpoint); 
                    if (right_is_leaf) { 
                         spanning_tree_leaf_pvs.insert(st_endpoint); 
                    }
               } 
          } 
     }
};


struct block_transfer_state
{
     block_transfer_state(building_block *bb, building_block *bb2,
                          map<int,int>::iterator mit, Adjacency& ajs, 
                          AdjacencyIterator ait)
          : b(bb), b2(bb2), miter(mit), adjs(ajs),
                                                   aiter(ait) {}
     building_block *b;  // the leaf being deleted
     building_block *b2; // its connection
     map<int,int>::iterator miter;
     Adjacency& adjs;
     AdjacencyIterator aiter;
};

struct bts_lth
{
     int operator()(const block_transfer_state* b1, 
                    const block_transfer_state* b2) const
     {
          return b1->b->id < b2->b->id;
     }
};

typedef set<block_transfer_state*, bts_lth> BTSset;

BTSset unfinished_block_transfers;

typedef map<int, building_block*> BBmap;

typedef map<int, pair<int,int> > FosterEdgeMap;  
          // To avoid deadlock, we must allow refinements to happen in some
          // cases in which a purgatory has not been fully resolved.  If a bb 
          // is a leaf, and its single spanning tree edge has been resolved
          // (which we can guarantee within 2p baskets via chair lift protocol),
          // the spanning tree within that purgatory system will be fixed to
          // the extent that we know a leaf (C) and could refine it away from
          // the building block (A).  Suppose
          // that v is in our A-C purgatory, there is an edge (v,w), and w is in
          // another purgatory in the same local component.  Suppose now that
          // v is resolved into C, which is gone.  When v is resolved, 
          // we need to relabel the edge (v[C], w[??]) and send it
          // downstream.  If no part has been resolved out of w's purgatory
          // system, then we are ok; we'll send the edge (v[C], w[LC])
          // downstream, were LC is our local component.  Suppose, however,
          // that some part, say D, has been refined away from w's purgatory
          // system before w has been resolved.  We don't know where w is going
          // to end up, so we don't know how to relabel w.  
          // Until w is resolved, (v,w) is a "foster edge" of this
          // processor, and it is stored in w's foster edge list.  When w is
          // finally resolved, its foster edges will be sent downstream.
          // Mites come into play in two ways here.  
          // Mites(1): When v gets resolved, there may be a non-constant number
          //           of w_i's whose foster edge lists need augmenting.
          //            * make a map entry indicating that v has been resolved 
          //              into C (which is gone), and has generated mites
          //              to augment foster edge lists
          //            * launch Mites(1) who will augment these lists lazily.
          //            * suppose that w_i gets resolved into D, which has been
          //              sent downstream, before v's mite reaches w_i.  w_i
          //              will launch Mites(1) which eventually will want to put
          //              (v,w) into v's foster edge list.  v is gone.  However,
          //              v's map entry exists, and 
          //              we'll know via that map entry that v got resolved to
          //              C.  w_i's mite will not add anything to a list; it
          //              will simply send (w_i[D],v[C]) downstream.
          // Mites(2): When w_i gets resolved, it will launch mites to traverse
          //           its foster edge list, sending the now-complete edge
          //           relabelings downstream.
          //            * TODO, describe this protocol

void new_building_block(int id, int size, DJsets& my_sets, 
                        BBmap& bbmap, bool already_have_djset=false)
{
     // in the case of accepting a refinement, there will already be an
     // entry for this set in the DJset structure, and we want to retain
     // that entry.
     if (!already_have_djset) {
          my_sets.make_set(id, size);
     }
     bbmap[id] = new building_block(id, size);
}

void print_spanning_tree_leaves(
                        map<int,building_block*>& prim_vertex2building_block)
{
     printf("spanning tree leaves: (");
     for (set<int>::iterator sit=spanning_tree_leaf_pvs.begin(); 
          sit!=spanning_tree_leaf_pvs.end(); sit++) 
     { 
          int pv = *sit;
          int bb_id = prim_vertex2building_block[pv]->id;
          printf("pv:%d[bb: %d] ", pv, bb_id);
     }
     printf(")\n");
}

void print_building_block(building_block& bb,
                        map<int,building_block*>& prim_vertex2building_block)
{
     printf("building_block(id:%d,size:%d\n", bb.id, bb.size);
     printf("spanning tree edges: (");
     map<int,set<int>* >::iterator it=bb.st_map.begin(); 
     for (; it!=bb.st_map.end(); it++) {
          int prim_v = (*it).first;
          set<int>* pv_edges = (*it).second;
          set<int>::iterator lit = pv_edges->begin();
          for (; lit!=pv_edges->end(); lit++) {
               int prim_w = *lit;
               int bb_w = prim_vertex2building_block[prim_w]->id;
               printf("(%d[%d], %d[%d]) ", prim_v, bb.id, prim_w, bb_w);
          }
     }
     printf("))\n");
}

void augment_spanning_tree(int prim_v, int prim_w, 
                           building_block* b1, building_block* b2)
{
     map<int,set<int>*>& b1_st_map = b1->st_map;
     map<int,set<int>*>& b2_st_map = b2->st_map;
     if (b1_st_map.find(prim_v) != b1_st_map.end()) {
          b1_st_map[prim_v]->insert(prim_w);
     } else {
          b1_st_map[prim_v] = new set<int>;
          b1_st_map[prim_v]->insert(prim_w);
     }
     if (b2_st_map.find(prim_w) != b2_st_map.end()) {
          b2_st_map[prim_w]->insert(prim_v);
     } else {
          b2_st_map[prim_w] = new set<int>;
          b2_st_map[prim_w]->insert(prim_v);
     }
}

bool spanning_tree_leaf(map<int,set<int>*>& st_map, int& prim_v)
{
     if (st_map.size() != 1) {
          return false;
     }
     map<int,set<int>*>::iterator it = st_map.begin(); 
     set<int>* st_neighbors = (*it).second;
     if (st_neighbors->size() == 1) {
          prim_v = (*it).first;
          return true;
     }
     return false;
}

bool no_spanning_tree_edge(map<int,set<int>*>& st_map, int& prim_v)
{
     return (st_map.size() == 0);
}

int get_a_spanning_tree_endpoint(building_block *b)
{
     map<int,set<int>*>& st_map = b->st_map;
     if (st_map.size() == 0) {
          return -1;
     }
     map<int,set<int>*>::iterator it = st_map.begin(); 
     if (it != st_map.end()) {
          return (*it).first;
     }
     return -1;
}

void delete_spanning_tree_edge(int prim_v, int prim_w, 
                        Graph& primitive_edges,
                        DJsets& my_sets,
                        BBmap& bbmap,
                        map<int,building_block*>& prim_vertex2building_block)
{
     building_block* b = prim_vertex2building_block[prim_v];
     building_block* b2 = prim_vertex2building_block[prim_w];
     int b_st_endpoint=0;
     int b2_st_endpoint=0;
     map<int,set<int>*>& b_st_map = b->st_map;
     map<int,set<int>*>& b2_st_map = b2->st_map;
     bool b_was_leaf = spanning_tree_leaf(b_st_map, b_st_endpoint);
     bool b2_was_leaf = spanning_tree_leaf(b2_st_map, b2_st_endpoint);
     if (b_was_leaf) {
          spanning_tree_leaf_pvs.erase(prim_v);
     }
     if (b2_was_leaf) {
          spanning_tree_leaf_pvs.erase(prim_w);
     }
     set<int>* b_pv_edges = b_st_map[prim_v];
     b_pv_edges->erase(prim_w);
     if (b_pv_edges->size() == 0) {
          b_st_map.erase(prim_v);
          sets_to_delete.push_back(b_pv_edges); }
     set<int>* b2_pw_edges = b2_st_map[prim_w];
     b2_pw_edges->erase(prim_v);
     if (b2_pw_edges->size() == 0) {
          b2_st_map.erase(prim_w);
          sets_to_delete.push_back(b2_pw_edges);
     }
     // if this was a temporary spanning tree edge introduced during purgatory
     // processing, delete the associated temporary primitive edge.
     if (prim_v < 0 || prim_w < 0) {
          primitive_edges.remove_edge(prim_v,prim_w);
     }
     bool b_is_leaf = spanning_tree_leaf(b_st_map, b_st_endpoint);
     bool b2_is_leaf = spanning_tree_leaf(b2_st_map, b2_st_endpoint);
     if (b_is_leaf) {
          spanning_tree_leaf_pvs.insert(b_st_endpoint);
     }
     if (b2_is_leaf) {
          spanning_tree_leaf_pvs.insert(b2_st_endpoint);
     }
}

void resolve_primitive_vertex(int prim_v, int dest_bb, int time,
                        Graph& primitive_edges,
                        DJsets& my_sets,
                        BBmap& bbmap,
                        map<int,building_block*>& prim_vertex2building_block)
{
     //     We are resolving prim_v out of purg into destination. 
     //     The latter needs to receive the list of spanning tree edges
     //     incident on prim_v. Since these lists are keyed on primitive
     //     vertex names, not building_block pointers, we can give the 
     //     set of st-destination primitive vertex id's to the destination
     //     of this resolution step.
     //
     building_block * destination = bbmap[dest_bb];
     building_block * purg = prim_vertex2building_block[prim_v];
     map<int,set<int>*>& purg_st_map = purg->st_map;
     map<int,set<int>*>& dest_st_map = destination->st_map;
     // move spanning tree edges
     if (purg_st_map.find(prim_v) != purg_st_map.end()) {
          set<int>* b_pv_edges = purg_st_map[prim_v];
          assert(dest_st_map.find(prim_v) == dest_st_map.end());
          dest_st_map[prim_v] = b_pv_edges;
          purg_st_map.erase(prim_v);
          prim_vertex2building_block[prim_v] = destination;
     }
     // update local view
     purg->known_vertices.erase(prim_v);
     destination->known_vertices[prim_v] = time;    // prim_v lives in b now
     // Test to see if the destination is fully resolved
     // This test will only succeed if there are no hidden vertices, but it's
     // worth trying anyway.
     purg->mite_decrement = 1;
     mite_set.insert(purg);
}

bool loading_dock_transfer(block_transfer_state* bts, int work_to_do,
                           LoadingDock& loading_dock, Graph& primitive_edges,
                           BBmap& bbmap)
{
     int work_done = 0;
     building_block *b = bts->b;
     building_block *b2 = bts->b2;
     map<int,int>& b_prim_v = b->known_vertices;
     map<int,int>::iterator& miter = bts->miter;
     bool work_complete = true;
     for (; miter!=b_prim_v.end() && work_done < work_to_do; miter++) {
          int v = (*miter).first;
          Adjacency& adjs = bts->adjs;
          AdjacencyIterator& aiter = bts->aiter;
          for ( ; aiter != adjs.end() && work_done < work_to_do; aiter++) { 
               pair<int, int> p = *aiter; 
               int w = p.first; 
               int timestamp = p.second;
               loading_dock.push_back(parcel(v,w,b->id,b2->id,b->size,
                                             b2->size,timestamp));
               primitive_edges.remove_edge(v,w);
               work_done++;
               if (work_done >= work_to_do) {
                    work_complete = false;
                    break;
               }
          }
     }
     if (work_complete) {
          unfinished_block_transfers.erase(bts);
          delete bts;
     }
               
     // We are done sending the cut for the refinement of b.
     // By definition, b can't be a purgatory, so it's ready to go.
     if (!b->local_component_leader) {
          bbmap.erase(b->id);
          delete b;
     }
     return work_complete;
}

void delete_leaf(building_block* b, int time, DJsets& my_sets, BBmap& bbmap,
                           Graph& primitive_edges, LoadingDock& loading_dock,
                           map<int,building_block*>& prim_vertex2building_block)
{
     map<int,set<int>*>& b_st_map = b->st_map;
     int b_size = b->size;
     int prim_v = 0; // should be numerical_limits..max
     if (!spanning_tree_leaf(b_st_map, prim_v)) {
          fprintf(stderr, "error: delete_leaf called on non-leaf\n");
          exit(1);
     }
     // This routine updates the leaf list of the local component of b, and
     // the spanning tree lists of the endpoints of the edge connecting the 
     // leaf (b) being deleted.  Note that it does not affect the disjoint
     // set representation.  b might be the leader of the local component,
     // and even after its "deletion," it will retain that identity in order
     // to relabel edges consistently.
     // b2 is the building block adjacent to leaf b;
     int lc_id = my_sets.find_set(b->id);
     building_block *lc = bbmap[lc_id];
     map<int,int>& b_prim_v = b->known_vertices;
     // remove b from the leaf list of lc
     spanning_tree_leaf_pvs.erase(prim_v);
     // fix the spanning tree
     pair<int,set<int>*> p = *(b_st_map.begin());   // (prim_v, far_bb verts)
     set<int>* b_st_set = p.second;
     // only one element; get it
     set<int>::iterator st_e = b_st_set->begin();
     int prim_w = *st_e;
     building_block *b2 = prim_vertex2building_block[prim_w];
     // Deal with getting the first refinement edge out and priming
     // the loading dock to continue with the others.
     map<int,int>::iterator miter = b_prim_v.begin();
     int v = (*miter).first;
     Adjacency& adjs = primitive_edges.vertices[v];
     AdjacencyIterator aiter = adjs.begin(); 
     block_transfer_state *bts = 
          new block_transfer_state(b, b2, miter, adjs, aiter); 
     // do one unit of work immediately: the refinement message.
     // TODO: make sure this takes priority and goes out in the same
     //       basket we received to initiate this refinement. We'll
     //       need a different routine (or parameter) to provide instant 
     //       gratification rather than appending to a loading dock.
     bool done=loading_dock_transfer(bts, 1, loading_dock, 
                                     primitive_edges, bbmap);
     if (!done) { 
          unfinished_block_transfers.insert(bts);
     }

     delete_spanning_tree_edge(prim_v, prim_w, primitive_edges, my_sets,bbmap,
                               prim_vertex2building_block);
     // account for b leaving lc
     lc->size -= b_size;

     // we don't worry about removing the set from the disjoint set structure.
     // but we do worry about reclaiming the space of the building_block.
     // DJset leaders never die; they live to provide identities.
}

void link_local_components(int pv, int pw, int bb1, int bb2, 
                           DJsets& my_sets, BBmap& bbmap,
                           int active_refine=0)
{
     int lc1 = my_sets.find_set(bb1);
     int lc2 = my_sets.find_set(bb2);
     my_sets.set_union(lc1, lc2);
     int new_leader;
     int bb_in_new_leader;
     int new_follower;
     int bb_in_new_follower;
     if (my_sets.find_set(lc1) == lc1) {
          new_leader = lc1;
          bb_in_new_leader = bb1;
          new_follower = lc2;
          bb_in_new_follower = bb2;
     } else {
          new_leader = lc2;
          bb_in_new_leader = bb2;
          new_follower = lc1;
          bb_in_new_follower = bb1;
     }
     building_block *bb_in_nl = bbmap[bb_in_new_leader];
     building_block *bb_in_nf = bbmap[bb_in_new_follower];
     building_block* new_leader_bb = bbmap[new_leader];
     building_block* new_follower_bb = bbmap[new_follower];
     printf("link_local_components: new leader: %d, new follower: %d\n", 
            new_leader_bb->id, new_follower_bb->id);
     new_leader_bb->local_component_leader = true;
     new_follower_bb->local_component_leader = false;

     map<int, set<int>*>& b_st_map = bb_in_nl->st_map;
     map<int, set<int>*>& b2_st_map = bb_in_nf->st_map;
     int l_endpoint = 0;
     int f_endpoint = 0;

     if (!active_refine) {
          bool leader_was_leaf = spanning_tree_leaf(b_st_map, l_endpoint); 
          bool follower_was_leaf=spanning_tree_leaf(b2_st_map,f_endpoint);
          if (leader_was_leaf) { 
               spanning_tree_leaf_pvs.erase(l_endpoint); 
          } 
          if (follower_was_leaf) { 
               spanning_tree_leaf_pvs.erase(f_endpoint); 
          }
     }

     augment_spanning_tree(pv, pw, bbmap[bb1], bbmap[bb2]);

     if (!active_refine) { 
          bool leader_is_leaf = spanning_tree_leaf(b_st_map, l_endpoint); 
          bool follower_is_leaf=spanning_tree_leaf(b2_st_map,f_endpoint); 
          if (leader_is_leaf) { 
               spanning_tree_leaf_pvs.insert(l_endpoint); 
          } 
          if (follower_is_leaf) { 
               spanning_tree_leaf_pvs.insert(f_endpoint); 
          }
     }
}


// filling mode
void add_primitive_edge(int prim_v, int prim_w, int bb_v, int bb_w, 
                        int bb_v_size, int bb_w_size, int time,
                        Graph& primitive_edges,
                        map<int,building_block*>& prim_vertex2building_block,
                        DJsets& my_sets,
                        BBmap& bbmap,
                        bool active_refinement=false)
{
     if (!my_sets.set_exists(bb_v)) {
          new_building_block(bb_v, bb_v_size, my_sets, bbmap);
     }
     if (!my_sets.set_exists(bb_w)) {
          new_building_block(bb_w, bb_w_size, my_sets, bbmap);
     }
     prim_vertex2building_block[prim_v] = bbmap[bb_v];
     prim_vertex2building_block[prim_w] = bbmap[bb_w];
     bbmap[bb_v]->known_vertices[prim_v] = time;
     bbmap[bb_w]->known_vertices[prim_w] = time;
     // refinement is implemented with the gadget of introducing "fake edges"
     // that hold the damaged spanning tree together until resolution.  We
     // want to acknowledge the existence of these edges, but we don't want 
     // them to induce any disjoint set operations.  We know that they are
     // in the same local component.
     int bb_v_p = my_sets.find_set(bb_v);
     int bb_w_p = my_sets.find_set(bb_w);
     if ((my_sets.find_set(bb_v)!=my_sets.find_set(bb_w)) || active_refinement){
          link_local_components(prim_v, prim_w, bb_v, bb_w, my_sets, bbmap,
                                active_refinement);
          // TODO:  in refinement, we actually need to do this, but we
          //        need to be careful about leaf lists.
     }
     primitive_edges.add_edge(prim_v,prim_w, time);
}

void accept_refinement(int prim_v, int prim_w,
                           int c, int a,
                           int size_c, int new_size_a, int timestamp, 
                           Graph& primitive_edges, 
                           map<int,building_block*>& prim_vertex2building_block,
                           DJsets& my_sets, BBmap& bbmap)
{
     // refine C away from A

     // If A is a non-purgatory, then we have to make it a purgatory (including
     // renaming), then create new building blocks A and C.
     //
     // first, rename A to be a purgatory (name -1, -2, etc.(next_purgatory_id))
     building_block* origA = bbmap[a];
     int size_a = origA->size;
     bbmap.erase(a);  // note: bb ptr was just saved, will be reused

     // if A was a leaf, remove it from the leaf set
     // We'll need to resolve the purgatory before knowing a true leaf.
     // This is where a transition to a "bad leaves" set would happen.
     int st_endpoint;
     if (spanning_tree_leaf(origA->st_map, st_endpoint)) {
          spanning_tree_leaf_pvs.erase(st_endpoint);
     }
     // A changes to be a purgatory bb
     origA->id = --next_purgatory_id;
     bbmap[origA->id] = origA;
     // num_to_resolve is the total number of vertices that need to be
     // resolved from this purgatory or any of its purgatory ancestors
     origA->num_to_resolve = origA->known_vertices.size();
     if (origA->parent_purgatory != NULL) {
          origA->num_to_resolve += origA->parent_purgatory->num_to_resolve;
     }
     my_sets.make_set(origA->id, new_size_a + size_c);
     // the new A will be the original A minus C
     // create two new building blocks (for C and the new A)
     new_building_block(a, new_size_a, my_sets, bbmap, ALREADY_HAVE_DJSET);
     new_building_block(c, size_c, my_sets, bbmap);
     building_block* newA = bbmap[a];
     building_block* C = bbmap[c];
     // hook these up in a tree structure (to be crawled by mites)
     newA->parent_purgatory = origA;
     C->parent_purgatory = origA;
     origA->left_child = C;
     origA->right_child = newA;
     // need to resolve prim_v, prim_w
     // add the refinement edge to the graph stored by this processor
     add_primitive_edge(prim_v,prim_w,C->id,newA->id,size_c,new_size_a,
                        timestamp, primitive_edges, 
                        prim_vertex2building_block,
                        my_sets, bbmap, ACTIVE_REFINEMENT);
     // make sure that local component lookups will still be correct
     int lc1 = my_sets.find_set(origA->id);
     int lc2 = my_sets.find_set(newA->id);
     printf("accept refinement: linking %d and %d\n", origA->id, newA->id);
     my_sets.set_union(lc1, lc2);
     lc1 = my_sets.find_set(origA->id); 
     lc2 = my_sets.find_set(newA->id); 
     printf("accept refinement: result: leader(%d): %d, leader(%d): %d\n", 
               origA->id, lc1, newA->id, lc2);
     lc2 = my_sets.find_set(C->id);
     printf("accept refinement: linking %d and %d\n", lc1, lc2);
     my_sets.set_union(lc1, lc2);
     lc1 = my_sets.find_set(origA->id); 
     lc2 = my_sets.find_set(C->id); 
     printf("accept refinement: result: leader(%d): %d, leader(%d): %d\n", 
               origA->id, lc1, C->id, lc2);
     // fill the same basket that contained the first announcement of this
     // refinement with a resolution request to begin fixing the spanning tree.
     // Pick a vertex from the new purgatory with a spanning tree edge.
     // TODO: currently, resolution occurs only when num_to_resolve goes to
     // zero.  If we maintain the number of vertices with incident spanning
     // tree edge that need to be resolved, then resolution (and leaf status
     // update) could happen before num_to_resolve falls to zero.
     int resolution_request_v = get_a_spanning_tree_endpoint(origA);
     assert(resolution_request_v >= 0); // otherwise the spanning tree was
                                        // already broken 
     // no loading dock; fill the basket (once we have that concept) 
     // and send immediately.  
     printf("initiating resolution request for %d\n", resolution_request_v);
}

void activation(BTSset& unfinished_block_transfers,
                LoadingDock& loading_dock,
                Graph& primitive_edges,
                BBmap& bbmap)
{
     // advance constant chunks of work 
     // current: loading_dock_transfer, 
     // future: lazy tree mite crawling.  
     // Do this before each command (add edge, etc).
     printf("activation(): mite set size: %d\n", mite_set.size());

     // loading dock work
     BTSset::iterator btsiter =
          unfinished_block_transfers.begin();
     if (btsiter != unfinished_block_transfers.end()) {
          block_transfer_state* bts = *btsiter; 
          loading_dock_transfer(bts, WORK_CHUNK, loading_dock,
                                primitive_edges, bbmap);
     }

     // lazy tree mite work
     for (int i=0; i<WORK_CHUNK; i++) {
          if (mite_set.size() > 0) {
               set<building_block*>::iterator mit = mite_set.begin(); 
               building_block* b = *mit;
               printf("activation: work chunk %d: before: "
                      "(bid: %d: num_to_resolve: %d)\n", i, b->id, 
                      b->num_to_resolve);
               b->mite_visit(); 
               printf("activation: work chunk %d: after: "
                      "(bid: %d: num_to_resolve: %d)\n", i, b->id, 
                      b->num_to_resolve);
               if (b->num_to_resolve == 0 && b->id < 0) { // resolved purgatory
                    printf("activation: deciding whether to delete %d\n",b->id);
                    if (!b->local_component_leader) { 
                         printf("activation: deleting %d\n", b->id);
                         bbmap.erase(b->id); 
                         delete b;
                    }
               }
               // if (destination->known_vertices.size() == destination->size) 
               // this building block is now resolved.  If it is a leaf, add
               // to its component's leaf_list (purg was already deleted from
               // that list).  
               // We also know that any remaining vertices from traversing our 
               // purgatory parent down to our level will go to our sibling.  
               // Set up some mites to start that transfer (probably calling 
               // this routine.
          } else {
               break;
          }
     }
}

int main(int narg, char **arg)
{
     // simulate filling processor
     Graph primitive_edges;
     map<int,building_block*> prim_vertex2building_block;
     // ***************************************************************
     // ** refine_loading_dock interacts with bbmap as follows: when
     // ** a building block is refined away, we don't copy all of its
     // ** edges to a loading dock, as that would be a non-constant time
     // ** operation. Rather, we simply switch the block from bbmap to
     // ** refine_loading_dock.  Whenever there is an empty basket, we
     // ** pluck from the latter.
     // ***************************************************************
     map<int, building_block*> refine_loading_dock;
     DJsets my_sets;
     BBmap bbmap;
     LoadingDock loading_dock;
     char cmd;
     int pv, bbv, sizev, pw, bbw, sizew, time;

     while (cin >> cmd) { 
          cmd = toupper(cmd);
          switch (cmd) {
          case 'E': cin >> pv >> bbv >> sizev >> pw >> bbw >> sizew >> time; 
                    printf("E: ADD EDGE((%d,%d), (%d,%d), (%d,%d): %d)\n",
                           pv, pw, bbv, bbw, sizev, sizew, time);
                    add_primitive_edge(pv,pw,bbv,bbw,sizev,sizew,time,
                                       primitive_edges, 
                                       prim_vertex2building_block, 
                                       my_sets, bbmap);
                    break;
          case 'D': cin >> bbv >> time; 
                    printf("D: DELETE BUILDING BLOCK(%d)\n", bbv);
                    delete_leaf(bbmap[bbv], time, my_sets, bbmap, 
                                          primitive_edges, loading_dock,
                                          prim_vertex2building_block);
                    break;
          case 'P': { 
                         printf("----------------------------------\n");
                         BBmap::iterator blocks = bbmap.begin(); 
                         for (; blocks != bbmap.end(); blocks++) { 
                              building_block* b = (*blocks).second; 
                              print_building_block(*b,
                                                   prim_vertex2building_block); 
                         }
                         print_spanning_tree_leaves(prim_vertex2building_block);
                         printf("----------------------------------\n");
                    }
                    printf("primitive edges (adj:timestamp)---\n");
                    printf("----------------------------------\n");
                    primitive_edges.print();
                    printf("----------------------------------\n");
                    break;
          case 'R': {cin >> pv >> bbv >> sizev >> pw >> bbw >> sizew >> time; 
                    printf("R: REFINE((%d,%d), (%d,%d), (%d,%d)\n",
                           pv, pw, bbv, bbw, sizev, sizew, time);
                    building_block *bv = bbmap[bbv];
                    building_block *bw = bbmap[bbw];
                    accept_refinement(pv, pw, bbv, bbw,
                           sizev, sizew, time, 
                           primitive_edges, 
                           prim_vertex2building_block,
                           my_sets, bbmap);
                    }
                    break; 
          case 'S': cin >> pv >> bbv >> time; 
                    printf("S: RESOLVE(%d,%d): %d\n",
                           pv, bbv, time);
                    resolve_primitive_vertex(pv, bbv, time,
                           primitive_edges, 
                           my_sets, bbmap, prim_vertex2building_block);
                    break;
          case 'M': printf("M: MESSAGES READY TO SEND\n");
                    printf("----------------------------------\n");
                    list<parcel>::iterator dockit = loading_dock.begin();
                    for (; dockit!=loading_dock.end(); dockit++) {
                         parcel& p = *dockit;
                         p.print();
                    }
                    printf("----------------------------------\n");
                    break;
          }
          activation(unfinished_block_transfers, loading_dock, 
                     primitive_edges, bbmap);
     }
     return 0;
}
