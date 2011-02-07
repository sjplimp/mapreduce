#include "keyvalue.h"
using namespace MAPREDUCE_NS;

/* ----------------------------------------------------------------------
   rmat_stats
   print # of rows with a specific # of nonzeroes
------------------------------------------------------------------------- */

void rmat_stats(uint64_t itask, char *key, int keybytes, char *value,
		int valuebytes, KeyValue *kv, void *ptr)
{
  int *total = (int *) ptr;
  int nnz = *(int *) key;
  int ncount = *(int *) value;
  *total += ncount;
  printf("%d rows with %d nonzeroes\n",ncount,nnz);
}
