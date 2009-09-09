// Shared data types for passing keys and values.
// 
// Note that the overloaded ostream functions are really very slow, at least
// when writing to files.  Writing a small example took more than twice as
// long using these on my mac compared to fprintf. Making them "inline" didn't
// help.   They are easy-to-use for debugging info to stdout, however.

#ifndef _SHARED_HPP
#define _SHARED_HPP

#define MRMEMSIZE 512
#include <stdint.h>
#include <iostream>


// Vertex made up of two 8-byte hashkey IDs.
class VERTEX16 {
public:
  uint64_t v[2];
  VERTEX16& operator=(const VERTEX16& rhs){
    v[0] = rhs.v[0]; 
    v[1] = rhs.v[1];
    return *this;
  };
  friend std::ostream& operator<< (std::ostream& output, const VERTEX16& v) {
    output << v.v[0] << " " << v.v[1];
    return output;
  };
  inline bool valid() {return (v[0] != 0);};
  inline void reset() {v[0] = 0; v[1] = 0;};
  friend bool operator<(const VERTEX16& lhs, const VERTEX16& rhs) {
    if (lhs.v[0] < rhs.v[0]) return true;
    if (lhs.v[0] > rhs.v[0]) return false;
    if (lhs.v[1] < rhs.v[1]) return true;
    return false;
  };
  friend bool operator!=(const VERTEX16& lhs, const VERTEX16& rhs) {
    if (lhs.v[0] != rhs.v[0]) return true;
    if (lhs.v[1] != rhs.v[1]) return true;
    return false;
  };
};


// Vertex made up of one 8-byte hashkey ID.
class VERTEX08{
public:
  uint64_t v[1];
  VERTEX08& operator=(const VERTEX08& rhs){v[0] = rhs.v[0]; return *this;};
  friend std::ostream& operator<< (std::ostream& output, const VERTEX08& v) {
    output << v.v[0];
    return output;
  };
  inline bool valid() {return (v[0] != 0);};
  inline void reset() {v[0] = 0;};
  friend bool operator<(const VERTEX08& lhs, const VERTEX08& rhs) {
    if (lhs.v[0] < rhs.v[0]) return true;
    return false;
  };
  friend bool operator!=(const VERTEX08& lhs, const VERTEX08& rhs) {
    if (lhs.v[0] != rhs.v[0]) return true;
    return false;
  };
};

// Vertex in 1-N ordering; occasionally we use negative values for
// identifying special cases, so make sure this type is signed.
class iVERTEX{
public:
  int v;
  iVERTEX& operator=(const iVERTEX& rhs){v = rhs.v; return *this;};
  friend std::ostream& operator<< (std::ostream& output, const iVERTEX& v) {
    output << v.v;
    return output;
  };
  inline bool valid() {return (v != 0);};
  inline void reset() {v = 0;};
  friend bool operator<(const iVERTEX& lhs, const iVERTEX& rhs) {
    if (lhs.v < rhs.v) return true;
    return false;
  };
  friend bool operator!=(const iVERTEX& lhs, const iVERTEX& rhs) {
    if (lhs.v != rhs.v) return true;
    return false;
  };
};

// Edge Weight type; set to 
//   number of occurrence of edge in original input OR
//   1/number of occurrences of edge.
typedef double WEIGHT;

// Edge with destination vertex (16-bytes) and edge weight
// Note:  edge_label1 assumes v is first field of this struct.
class EDGE16{
public:
  VERTEX16 v;
  WEIGHT wt;  
  friend std::ostream& operator<<(std::ostream& output, const EDGE16& e) {
    output << e.v << " " << e.wt;
    return output;
  };
};

// Edge with destination vertex (8-bytes) and edge weight
// Note:  edge_label1 assumes v is first field of this struct.
class EDGE08{
public:
  VERTEX08 v;
  WEIGHT wt;  
  friend std::ostream& operator<<(std::ostream& output, const EDGE08& e) {
    output << e.v << " " << e.wt;
    return output;
  };
};

// Edge with destination vertex (VERTEXTYPE) and edge weight
// Note:  edge_reverse assumes v is first field of this struct.
class iEDGE{
public:
  iVERTEX v;
  WEIGHT wt;
  friend std::ostream& operator<<(std::ostream& output, const iEDGE& e) {
    output << e.v << " " << e.wt;
    return output;
  }
};

// Label needed for renumbering vertices from hashkey IDs to [1:N].
typedef struct {
  int nthresh;
  int count;
} LABEL;

#endif
