// Shared data types for passing keys and values.

#ifndef _SHARED_HPP
#define _SHARED_HPP

// Vertex type in 1-N ordering; occasionally we use negative values for
// identifying special cases, so make sure this type is signed.
typedef int VERTEX;

typedef struct {
  VERTEX nthresh;
  VERTEX count;
} LABEL;

// Edge Weight type; set to number of occurrence of edge in original input.
typedef int WEIGHT;

// Edge with destination vertex (16-bytes) and edge weight
// Note:  edge_label1 assumes v is first field of this struct.
typedef struct {
  uint64_t v[2];
  WEIGHT wt;  
} EDGE16;

// Edge with destination vertex (8-bytes) and edge weight
// Note:  edge_label1 assumes v is first field of this struct.
typedef struct {
  uint64_t v[1];
  WEIGHT wt;  
} EDGE08;

// Edge with destination vertex (VERTEXTYPE) and edge weight
// Note:  edge_reverse assumes v is first field of this struct.
typedef struct {
  VERTEX v;
  WEIGHT wt;
} EDGE;

#endif
