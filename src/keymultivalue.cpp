/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

#include "mpi.h"
#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "stdint.h"
#include "keymultivalue.h"
#include "keyvalue.h"
#include "hash.h"
#include "memory.h"

using namespace MAPREDUCE_NS;

#define MIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

#define KEYCHUNK 1000000

/* ---------------------------------------------------------------------- */

KeyMultiValue::KeyMultiValue(MPI_Comm caller)
{
  memory = new Memory(caller);

  nbuckets = 1;
  hashmask = nbuckets-1;
  buckets = new int[nbuckets];
  buckets[0] = -1;
  maxdepth = 0;

  nkey = maxkey = 0;
  keys = NULL;
  valueindex = NULL;

  cloned = 0;
  collapsed = 0;
  singlekey = NULL;
}

/* ----------------------------------------------------------------------
   free all memory
------------------------------------------------------------------------- */

KeyMultiValue::~KeyMultiValue()
{
  delete memory;
  delete [] buckets;
  memory->sfree(keys);
  delete [] valueindex;
  delete [] singlekey;
}

/* ----------------------------------------------------------------------
   create KMV pairs with unique keys from a KV with non-unique keys
   each processor works on only its data
------------------------------------------------------------------------- */

void KeyMultiValue::create(KeyValue *kv)
{
  int kv_nkey = kv->nkey;
  int *kv_keys = kv->keys;
  char *keydata = kv->keydata;

  delete [] valueindex;
  valueindex = new int[kv_nkey];
  for (int i = 0; i < kv_nkey; i++) valueindex[i] = -1;

  for (int i = 0; i < kv_nkey; i++) {
    char *key = &keydata[kv_keys[i]];
    int keylen = kv_keys[i+1] - kv_keys[i];
    int ibucket = hash(key,keylen);
    int ikey = find(ibucket,key,keylen,kv);

    if (ikey < 0) {
      if (nkey == maxkey) {
	maxkey += KEYCHUNK;
	keys = (KeyEntry *)
	  memory->srealloc(keys,maxkey*sizeof(KeyEntry),"keys");
      }
      keys[nkey].keyindex = i;
      keys[nkey].count = 1;
      keys[nkey].valuehead = i;
      keys[nkey].next = -1;
      valueindex[i] = -1;
      nkey++;
      if (nkey > nbuckets) grow_buckets(kv);

    } else {
      keys[ikey].count++;
      valueindex[i] = keys[ikey].valuehead;
      keys[ikey].valuehead = i;
    }
  }
}

/* ----------------------------------------------------------------------
   double the number of hash buckets
   requires re-hashing all keys to put them in new buckets
------------------------------------------------------------------------- */

void KeyMultiValue::grow_buckets(KeyValue *kv)
{
  nbuckets *= 2;
  hashmask = nbuckets-1;
  maxdepth = 0;
  delete [] buckets;
  buckets = new int[nbuckets];
  for (int i = 0; i < nbuckets; i++) buckets[i] = -1;

  // rehash current KeyEntries

  int *kv_keys = kv->keys;
  char *keydata = kv->keydata;

  for (int i = 0; i < nkey; i++) {
    keys[i].next = -1;
    int ikey = keys[i].keyindex;
    char *key = &keydata[kv_keys[ikey]];
    int keylen = kv_keys[ikey+1] - kv_keys[ikey];
    int ibucket = hash(key,keylen);

    int depth = 1;
    if (buckets[ibucket] < 0) buckets[ibucket] = i;
    else {
      int iprevious;
      int ikey = buckets[ibucket];
      while (ikey >= 0) {
	iprevious = ikey;
	ikey = keys[ikey].next;
	depth++;
      }
      keys[iprevious].next = i;
    }
    maxdepth = MAX(maxdepth,depth);
  }
}

/* ----------------------------------------------------------------------
   find a HashEntry in ibucket that matches key
   return index of HashEntry
   if cannot find key, return -1
   if not found, also update pointers for HashEntry that will be added
------------------------------------------------------------------------- */

int KeyMultiValue::find(int ibucket, char *key, int keylen, KeyValue *kv)
{
  int ikey = buckets[ibucket];

  if (ikey < 0) {
    buckets[ibucket] = nkey;
    maxdepth = MAX(maxdepth,1);
    return -1;
  }
  
  int depth = 1;
  while (ikey >= 0) {
    int index = keys[ikey].keyindex;
    char *key2 = &kv->keydata[kv->keys[index]];
    int key2len = kv->keys[index+1] - kv->keys[index];
    if (keylen == key2len && match(key,key2,keylen)) return ikey;
    int inext = keys[ikey].next;
    if (inext < 0) keys[ikey].next = nkey;
    ikey = inext;
    depth++;
  }
  
  maxdepth = MAX(maxdepth,depth);
  return -1;
}

/* ----------------------------------------------------------------------
   check match of 2 keys
   return 1 if a match, else 0
------------------------------------------------------------------------- */

int KeyMultiValue::match(char *key1, char *key2, int keylen)
{
  for (int i = 0; i < keylen; i++)
    if (key1[i] != key2[i]) return 0;
  return 1;
}

/* ----------------------------------------------------------------------
   hash a key to a bucket
------------------------------------------------------------------------- */

int KeyMultiValue::hash(char *key, int keylen)
{
  uint32_t ubucket = hashlittle(key,keylen,0);
  int ibucket = ubucket & hashmask;
  return ibucket;
}
