/* ----------------------------------------------------------------------
   print matrix degree statistics to screen
   run() inputs:
     mr = one KV per edge = (Eij,NULL)
     nrow = # of rows/vertices in matrix
   run() outputs:
     mr is destroyed
     degree stats are printed out
     return elapsed time
 ------------------------------------------------------------------------- */

#include "stdint.h"
#include "stdlib.h"
#include "blockmacros.h"
#include "matrix_stats.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

double MatrixStats::run(MapReduce *mr, uint64_t nrow)
{
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  uint64_t nonzero = mr->map(mr,map_edge_vert,NULL);
  mr->collate(NULL);
  mr->reduce(reduce_sum,NULL);
  mr->map(mr,map_invert,NULL);
  mr->collate(NULL);
  mr->reduce(reduce_sum,NULL);
  mr->gather(1);
  mr->sort_keys(compare_uint64);

  int me;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  if (me == 0)
    printf("Stats for matrix with %lu nrows, %lu non-zeroes:\n",nrow,nonzero);
  uint64_t sum = 0;
  mr->verbosity = 0;
  mr->timer = 0;
  mr->map(mr,map_print,&sum);
  if (me == 0) printf("  %lu rows with 0 elements\n",nrow-sum);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  return tstop-tstart;
}

/* ---------------------------------------------------------------------- */

void MatrixStats::map_edge_vert(uint64_t itask, char *key, int keybytes, 
				char *value, int valuebytes, 
				KeyValue *kv, void *ptr)
{
  EDGE *edge = (EDGE *) key;
  kv->add((char *) &edge->vi,sizeof(VERTEX),NULL,0);
}

/* ---------------------------------------------------------------------- */

void MatrixStats::reduce_sum(char *key, int keybytes,
			     char *multivalue, int nvalues,
			     int *valuebytes, KeyValue *kv, void *ptr)
{
  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  kv->add(key,keybytes,(char *) &nvalues_total,sizeof(uint64_t));
}

/* ---------------------------------------------------------------------- */

void MatrixStats::map_invert(uint64_t itask, char *key, int keybytes, 
			     char *value, int valuebytes,
			     KeyValue *kv, void *ptr)
{
  kv->add(value,valuebytes,key,keybytes);
}

/* ---------------------------------------------------------------------- */

int MatrixStats::compare_uint64(char *p1, int len1, char *p2, int len2)
{
  uint64_t i1 = *(uint64_t *) p1;
  uint64_t i2 = *(uint64_t *) p2;
  if (i1 > i2) return -1;
  else if (i1 < i2) return 1;
  else return 0;
}

/* ---------------------------------------------------------------------- */

void MatrixStats::map_print(uint64_t itask, char *key, int keybytes, 
			    char *value, int valuebytes, 
			    KeyValue *kv, void *ptr)
{
  uint64_t ncount = *(uint64_t *) key;
  uint64_t nrow = *(uint64_t *) value;
  printf("  %lu rows with %lu elements\n",nrow,ncount);
  uint64_t *sum = (uint64_t *) ptr;
  *sum += nrow;
}
