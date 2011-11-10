#ifndef DISJOINT_SETS_HPP
#define DISJOINT_SETS_HPP

#include <map>

using namespace std;

template <class size_type>
class disjoint_sets {
public:
     typedef typename map<size_type,size_type>::iterator map_iterator;
     disjoint_sets() 
     {
     }
     // from CLRS pseudocode:

     bool set_exists(size_type x)
     {
          map_iterator mit;
          mit = p.find(x); 
          return (mit != p.end());
     }

     void make_set(size_type x, size_type init_size=1)
     {
          p[x] = x;
          rank[x] = init_size;
     }

     // the classical disjoint set structure doesn't support deletion
     // because we can't know whether deleting a specific vertex shatters
     // the remaining ones into different components.  However, in our
     // context, we have access to the graph and periodically we do delete
     // things, knowing that the deletion won't do this shattering (see
     // vertex_to_refine in pipe.cpp).  The remove_set routine merely updates 
     // the size of the disjoint set roots and removes the deleted vertex.
     void remove_set(size_type x, size_type bbsize)
     {
          size_type parent = find_set(x);
          rank[parent] = rank[parent] - bbsize;
          p.erase(x);
          rank.erase(x);
     }

     bool has_set(size_type x)
     {
          return (p.find(x) != p.end());
     }

     size_type find_set(size_type x)
     {
          if (!set_exists(x)) return -1;
          if (x != p[x]) {
               p[x] = find_set(p[x]);
          }
          return p[x];
     }

     size_type find_rank(size_type x)
     {
          if (x != p[x]) {
               p[x] = find_set(p[x]);
          }
          return rank[p[x]];
     }

     // sometimes the sizes of our building blocks can change out from under us
     // after our disjoint sets have been created.  In this case, we need to be
     // able to fix the size of a given component.  This is triggered by our 
     // vertex refinement.  The rank of the set representatives changes, and
     // all constituents will see this rank when asked.
     size_type assign_rank(size_type x, size_type r)
     {
          if (x != p[x]) {
               p[x] = find_set(p[x]);
          }
          rank[p[x]] = r;
     }

     void link(size_type x, size_type y)
     {
          map_iterator mit = p.find(x); 
          if (mit == p.end()) make_set(x);
          mit = p.find(y); 
          if (mit == p.end()) make_set(y);

          //
          // each vertex will know the size of its component
          //
          size_type combined = find_rank(x) + find_rank(y);

          if (find_set(x) == find_set(y)) return;

          if (find_rank(x) > find_rank(y)) {
               p[y] = x;
          } else {
               p[x] = y;
          }
          rank[find_set(x)] = combined;
     }

     void set_union(size_type x, size_type y)
     {
          link(find_set(x), find_set(y));
     }

     void print()
     {
          map_iterator mit = p.begin();
          for (; mit != p.end(); mit++) {
               pair<size_type,size_type> pr = *mit;
               printf("set[%lu]: %lu\n", pr.first, find_set(pr.first));
          } 
          mit = rank.begin();
          for (; mit != rank.end(); mit++) {
               pair<size_type,size_type> pr = *mit;
               printf("rank[%lu]: %lu\n", pr.first, find_rank(pr.first));
          } 
     }

private:
     map<size_type,size_type> p;
     map<size_type,size_type> rank;
};

#endif
