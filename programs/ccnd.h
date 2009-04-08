// Connected Components via MapReduce
// Karen Devine and Steve Plimpton, Sandia Natl Labs
// Nov 2008
//
// Identify connected components in a graph via MapReduce
// algorithm due to Jonathan Cohen.
// The algorithm treats all edges as undirected edges.
// No distances are computed.

#ifndef CCND_H
#define CCND_H

#include "mapreduce.h"
#include "keyvalue.h"
#include "test_cc_common.h"

extern void ConnectedComponentsNoDistances(MapReduce *, CC *, int*);

#endif
