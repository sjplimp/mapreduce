/* ----------------------------------------------------------------------
   print CC statistics to screen
   run() inputs:
     mr = one KV per vertex = (Vi,Zi), Zi = root vertex of CC
   run() outputs:
     mr is destroyed
     CC stats are printed out
     return elapsed time
 ------------------------------------------------------------------------- */

#include "stdint.h"
#include "stdlib.h"
#include "blockmacros.h"
#include "cc_stats.h"
#include "mapreduce.h"
#include "keyvalue.h"

using MAPREDUCE_NS::MapReduce;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

double CCStats::run(MapReduce *mr)
{
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  mr->map(mr,map_invert,NULL);
  mr->collate(NULL);
  mr->reduce(reduce_sum,NULL);
  mr->map(mr,map_invert,NULL);
  mr->collate(NULL);
  mr->reduce(reduce_sum,NULL);
  mr->gather(1);
  mr->sort_keys(compare_uint64);

  int me;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  if (me == 0) printf("Stats for CCs:\n");
  mr->verbosity = 0;
  mr->timer = 0;
  mr->map(mr,map_print,NULL);

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  return tstop-tstart;
}

/* ---------------------------------------------------------------------- */

void CCStats::map_invert(uint64_t itask, char *key, int keybytes, 
			 char *value, int valuebytes, 
			 KeyValue *kv, void *ptr)
{
  kv->add(value,valuebytes,key,keybytes);
}

/* ---------------------------------------------------------------------- */

void CCStats::reduce_sum(char *key, int keybytes,
			 char *multivalue, int nvalues,
			 int *valuebytes, KeyValue *kv, void *ptr)
{
  uint64_t nvalues_total;
  CHECK_FOR_BLOCKS(multivalue,valuebytes,nvalues,nvalues_total)
  kv->add(key,keybytes,(char *) &nvalues_total,sizeof(uint64_t));
}

/* ---------------------------------------------------------------------- */

int CCStats::compare_uint64(char *p1, int len1, char *p2, int len2)
{
  uint64_t i1 = *(uint64_t *) p1;
  uint64_t i2 = *(uint64_t *) p2;
  if (i1 > i2) return -1;
  else if (i1 < i2) return 1;
  else return 0;
}

/* ---------------------------------------------------------------------- */

void CCStats::map_print(uint64_t itask, char *key, int keybytes, 
			char *value, int valuebytes, 
			KeyValue *kv, void *ptr)
{
  uint64_t nvert = *(uint64_t *) key;
  uint64_t ncc = *(uint64_t *) value;
  printf("  %u CCs with %u vertices\n",ncc,nvert);
}

