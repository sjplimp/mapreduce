// Program to test case where a processor has zero KVs to convert to a KMV.
// Karen Devine, April 2010
//
// Generate ten KVs on each processor, all with the same key so they all map to
// one processor during aggregate().  Then call aggregate() and convert() 
// and see what happens.
//
// To build:  mpic++ -m64 -I../src zerokv.cpp -L../src -lmrmpi
// To run:    mpirun -np 2 a.out

#include <mpi.h>
#include <iostream>
#include <string.h>
#include "mapreduce.h"
#include "keyvalue.h"

using namespace MAPREDUCE_NS;

//////////////////////////////////////////////////////////////////////////////
// Generate KVs that all have the same key.
void all_to_one(int itask, KeyValue *kv, void *ptr)
{
  const int cnt = 10;
  const char *key = "kdd";
  int len = strlen(key) * sizeof(char);
  for (int i = 0; i < cnt; i++)
    kv->add((char *) key, len, NULL, 0);
}

//////////////////////////////////////////////////////////////////////////////
// Print the number of occurrences of key.
void count(char *key, int keysize, char *multivalue, int nvalues, 
           int *valuebytes, KeyValue *kv, void *ptr)
{
  std::cout << " AFTER CONVERT " << key << " has " << nvalues 
            << " occurrences" << std::endl;
}

//////////////////////////////////////////////////////////////////////////////
int main(int narg, char *arg[])
{
  MPI_Init(&narg, &arg);
  int me, np;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &np);

  MapReduce *mr = new MapReduce;
  mr->map(np, &all_to_one, NULL);
  std::cout << me << " AFTER MAP, MY nkv = " << mr->kv->nkv << std::endl;

  mr->aggregate(NULL);
  std::cout << me << " AFTER AGGREGATE, MY nkv = " << mr->kv->nkv << std::endl;

  mr->convert();

  mr->reduce(&count, NULL);

  delete mr;
  MPI_Finalize();
}
