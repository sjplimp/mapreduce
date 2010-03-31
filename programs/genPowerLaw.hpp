// MapReduce map subroutines that generate Jon Berry's power-law numbers.
// Also includes collate and reduce functions for redistributing words
// based on processor (for genPowerLaw_*_Redistrib functions).
//
// Description:
//   Given an integer N, generate
//   -  (N+1) * 2^N total numbers with
//   -  2^(N+1) - 1 unique numbers
//   -  such that there are 2^(N-k) occurrences of each number in range
//      [2^k - 1: 2^(k+1) - 2], for k = 0, 1, ..., N.
//
//   Integers are converted to strings (e.g., for word counting).
//
//   Several versions are available; for all functions, N is passed as the
//   void* data pointer.
//
//   -  genPowerLaw:  Issue number-of-processors tasks; each task
//      generates ((N+1) * 2^N) / nprocs words.
//      Emit ((N+1)*2^N)/nprocs KVs per task:
//        key = word, value = NULL.
//
//   -  genPowerLaw_Redistrib:  Generate same subsets as 
//      genPowerLaw; enable redistribution of words differently
//      among processors.
//      Emit ((N+1)*2^N)/nprocs KVs per task:
//        key = processor to which word should be sent, value = word
//
//   -  genPowerLaw_FileEquiv:  Issue N+1 tasks; 
//      each task generates the subset of
//      words associated with task k, k = 0, 1, ..., N, above.
//      Emit 2^N KVs per task:
//        key = word, value = NULL.
//
//   -  genPowerLaw_FileEquiv_Redistrib:  Generate same subsets as 
//      genPowerLaw_FileEquiv; enable redistribution of words more evenly 
//      among processors.
//      Emit 2^N KVs per task:
//        key = processor to which word should be sent, value = word
//
//   -  genPowerLaw_TaskPerWord:  Issue 2^(N+1)-1 tasks;
//      each task k issues all occurrences of word k.
//      Emit appropriate number of occurrences of word k KVs per task:
//        key = word = k, value = NULL.
//
//   -  genPowerLaw_TaskPerWord_Redistrib:  Generate same subsets as 
//      genPowerLaw_TaskPerWord; enable redistribution of words more evenly 
//      among processors.
//      Emit appropriate number of occurrences of word k KVs per task:
//        key = processor to which word should be sent, value = word

#include "mapreduce.h"
#include "keyvalue.h"
#include "keymultivalue.h"
#include "blockmacros.hpp"
#include "assert.h"

using namespace MAPREDUCE_NS;

void genPowerLaw(int, KeyValue *, void *);
void genPowerLaw_Redistrib(int, KeyValue *, void *);
void genPowerLaw_FileEquiv(int, KeyValue *, void *);
void genPowerLaw_FileEquiv_Redistrib(int, KeyValue *, void *);
void genPowerLaw_TaskPerWord(int, KeyValue *, void *);
void genPowerLaw_TaskPerWord_Redistrib(int, KeyValue *, void *);

void genPowerLaw_RedistributeReduce(char *, int, char *, int, int *, 
                                    KeyValue *, void *);
int genPowerLaw_IdentityHash(char *, int);

enum GenPowerLawEnum {
  GEN_DEFAULT,
  GEN_TASKPERWORD,
  GEN_FILEEQUIV
};

///////////////////////////////////////////////////////////////////////////
// Generate words according to power-law number distribution.
// Assuming number of tasks == number of processors.
// Input:  itask number 
// Output: (N+1)*2^N KVs
//         Key = word
//         Value = NULL

void genPowerLaw(int itask, KeyValue *kv, void *ptr)
{
  int nprocs, me;
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);

  int N = *((int *) ptr);
  uint64_t two_power_N = ((uint64_t)1 << N);
  uint64_t totalwords = (N+1) * two_power_N;
  uint64_t FREQ = totalwords / 10 / nprocs;
  static uint64_t progress = 0, nextprint = FREQ;

  // Compute the index of words to be emitted by this task.
  // (N+1)2^N total words emitted will be divided evenly among the tasks.
  double words_per_task = (double) totalwords / nprocs;
  uint64_t start = (uint64_t) ((double) itask * words_per_task);
  uint64_t end = (uint64_t) ((double) (itask+1) * words_per_task);

  // Emit words for each task.
  for (uint64_t i = start; i < end; i++) {
    // Convert index to appropriate word in power-law distribution.
    uint64_t filen = i / two_power_N;
    uint64_t noccur = ((uint64_t) 1 << (N-filen));
    uint64_t filestart = ((uint64_t)1 << filen) - 1;
    uint64_t local_offset = i - filen * two_power_N;
    uint64_t emitkey = filestart + local_offset / noccur;

    // Convert number to word.
    char key[32];
    sprintf(key, "%llu", emitkey);

    // Emit.
    kv->add(key, strlen(key)+1, NULL, 0);
    progress++;
    if (me == 0 && progress > nextprint) {
      printf("%d     genwords %llu\n", me, progress);
      nextprint += FREQ;
    }
  }
}
///////////////////////////////////////////////////////////////////////////
// Generate words according to power-law number distribution.
// Assuming number of tasks == number of processors.
// Input:  itask number 
// Output: (N+1)*2^N KVs
//         Key = processor number for redistribution
//         Value = word

void genPowerLaw_Redistrib(int itask, KeyValue *kv, void *ptr)
{
  int nprocs, me;
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);

  int N = *((int *) ptr);
  uint64_t two_power_N = ((uint64_t)1 << N);
  uint64_t totalwords = (N+1) * two_power_N;
  uint64_t FREQ = totalwords / 10 / nprocs;
  static uint64_t progress = 0, nextprint = FREQ;

  // Compute the index of words to be emitted by this task.
  // (N+1)2^N total words emitted will be divided evenly among the tasks.
  double words_per_task = (double) totalwords / nprocs;
  uint64_t start = (uint64_t) ((double) itask * words_per_task);
  uint64_t end = (uint64_t) ((double) (itask+1) * words_per_task);

  // Emit words for each task.
  for (uint64_t i = start; i < end; i++) {
    // Convert index to appropriate word in power-law distribution.
    uint64_t filen = i / two_power_N;
    uint64_t noccur = ((uint64_t) 1 << (N-filen));
    uint64_t filestart = ((uint64_t)1 << filen) - 1;
    uint64_t local_offset = i - filen * two_power_N;
    uint64_t emitkey = filestart + local_offset / noccur;

    // Convert number to word.
    char key[32];
    sprintf(key, "%llu", emitkey);

    // Map words to processors for redistribution.
    int proc = i % nprocs;

    // Emit.
    kv->add((char *) &proc, sizeof(int), key, strlen(key)+1);
    progress++;
    if (me == 0 && progress > nextprint) {
      printf("%d     genwords %llu\n", me, progress);
      nextprint += FREQ;
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
// Identity hashing function; hashes keys to the processor number in the key.
// Used for redistributing KVs with key = proc to the associated processor.
int genPowerLaw_IdentityHash(char *key, int keybytes)
{
  return *((int *) key);
}


/////////////////////////////////////////////////////////////////////////////
// Reduce function for redistributing words to processors.  
// Works with genPowerLaw_*_Redistrib.
// 
void genPowerLaw_RedistributeReduce(char *key, int keybytes, char *multivalue,
             int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  static uint64_t progress = 0;

  int me;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);

  uint64_t total_nvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, total_nvalues)
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  char *word = multivalue;
  for (int i = 0; i < nvalues; i++) {
    kv->add(word, valuebytes[i], NULL, 0);
    word += valuebytes[i];
  }

  progress += nvalues;
  if (me == 0) printf("%d     redistribute %llu words...\n", me, progress);

  END_BLOCK_LOOP
}

/////////////////////////////////////////////////////////////////////////////
// Sanity test for power law numbers.  Make sure the count is correct
// for each word.
// Input:  KMV with key = word, MV = count.  Should fit on one page!
//         Also inputs N and an error flag in ptr.
// Output: None.  error flag is incremented if a word's count is wrong.
//
class genPowerLaw_SanityStruct {
public:
  genPowerLaw_SanityStruct(int n) {nerrors=0; N = n;};
  ~genPowerLaw_SanityStruct() {};
  uint64_t nerrors;
  int N;
};

void genPowerLaw_SanityTest(char *key, int keybytes, char *multivalue,
                            int nvalues, int *valuebytes, KeyValue *kv, 
                            void *ptr) 
{
  assert(nvalues == 1);

  // Compute number of instances of the word to emit.
  genPowerLaw_SanityStruct *a = (genPowerLaw_SanityStruct *) ptr;
  int64_t word = atol(key);
  int N = a->N;
  int m;

  for (m = N; m >= 0; m--) 
    if ((word < (1<<(m+1))-1) && (word >= (1<<m)-1)) break;

  // There should be 2**(N-m) copies of word.
  uint64_t ncopies = (1<<(N-m));
  uint64_t count = *((uint64_t*) multivalue);
  if (ncopies != count) a->nerrors++;
}

/////////////////////////////////////////////////////////////////////////////
// generate words using Jon Berry's strategy.
// Words are just number strings.
// Generate 2**N 0s; 2**(N-1) 1s and 2s; 2**(N-2) 3s, 4s, 5s and 6s;
//          2**(N-3) 7s, 8s, 9s, 10s, 11s, 12s, 13s, and 14s; etc.
// Maxword string is 2**(N+1)-1-1.
// The initial distribution of words is identical to having read
// data from N+1 files; the number of tasks == the number of files.
void genPowerLaw_FileEquiv(int itask, KeyValue *kv, void *ptr)
{
  int N = *((int *) ptr);

  // Compute word range for this task.
  uint64_t maxword = (1<<(itask+1))-1;
  uint64_t minword = (1<<(itask))-1;
  uint64_t ncopies = (1<<(N-itask));

  for (uint64_t w = minword; w < maxword; w++) {
    char key[32];
    sprintf(key, "%llu", w);
    for (uint64_t i = 0; i < ncopies; i++) {
      kv->add(key, strlen(key)+1, NULL, 0);
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
// generate words using Jon Berry's strategy.
// Words are just number strings.
// Generate 2**N 0s; 2**(N-1) 1s and 2s; 2**(N-2) 3s, 4s, 5s and 6s;
//          2**(N-3) 7s, 8s, 9s, 10s, 11s, 12s, 13s, and 14s; etc.
// Maxword string is 2**(N+1)-1-1.
// The initial distribution of words is identical to having read
// N+1 files; the number of tasks == the number of files.
// Emit a processor ID as key, so that can redistribute words evenly
// by processor ID.
// Emit the word as the value.
void genPowerLaw_FileEquiv_Redistrib(int itask, KeyValue *kv, void *ptr)
{
  // itask is the word to be emitted.
  // Compute number of instances of the word to emit.
  static int count = 0;
  static int np;
  if (!count) MPI_Comm_size(MPI_COMM_WORLD, &np);

  int N = *((int *) ptr);

  // Compute word range for this task.
  uint64_t maxword = (1<<(itask+1))-1;
  uint64_t minword = (1<<(itask))-1;
  uint64_t ncopies = (1<<(N-itask));

  for (uint64_t w = minword; w < maxword; w++) {
    char key[32];
    sprintf(key, "%llu", w);
    for (uint64_t i = 0; i < ncopies; i++) {
      int proc = count % np;
      count++;
      kv->add((char *) &proc, sizeof(int), key, strlen(key)+1);
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
// generate words using Jon Berry's strategy.
// Words are just number strings.
// Generate 2**N 0s; 2**(N-1) 1s and 2s; 2**(N-2) 3s, 4s, 5s and 6s;
//          2**(N-3) 7s, 8s, 9s, 10s, 11s, 12s, 13s, and 14s; etc.
// Maxword string is 2**(N+1)-1-1.
// All instances of one word are generated per map task.
void genPowerLaw_TaskPerWord(int itask, KeyValue *kv, void *ptr)
{
  // itask is the word to be emitted.
  // Compute number of instances of the word to emit.
  int N = *((int *) ptr);
  int m;

  for (m = N; m >= 0; m--) 
    if ((itask < (1<<(m+1))-1) && (itask >= (1<<m)-1)) break;

  // Emit 2**(N-m) copies of itask.
  char key[32];
  sprintf(key, "%d", itask);
  uint64_t ncopies = (1<<(N-m));
  for (uint64_t i = 0; i < ncopies; i++) {
    kv->add(key, strlen(key)+1, NULL, 0);
  }
}

/////////////////////////////////////////////////////////////////////////////
// generate words using Jon Berry's strategy.
// Words are just number strings.
// Generate 2**N 0s; 2**(N-1) 1s and 2s; 2**(N-2) 3s, 4s, 5s and 6s;
//          2**(N-3) 7s, 8s, 9s, 10s, 11s, 12s, 13s, and 14s; etc.
// Maxword string is 2**(N+1)-1-1.
// All instances of one word are generated per map task.
void genPowerLaw_TaskPerWord_Redistrib(int itask, KeyValue *kv, void *ptr)
{
  // itask is the word to be emitted.
  // Compute number of instances of the word to emit.
  static int count = 0;
  static int np;
  if (!count) MPI_Comm_size(MPI_COMM_WORLD, &np);
  int N = *((int *) ptr);
  int m;

  for (m = N; m >= 0; m--) 
    if ((itask < (1<<(m+1))-1) && (itask >= (1<<m)-1)) break;

  // Emit 2**(N-m) copies of itask.
  char key[32];
  sprintf(key, "%d", itask);
  uint64_t ncopies = (1<<(N-m));
  for (uint64_t i = 0; i < ncopies; i++) {
    int proc = count % np;
    count++;
    kv->add((char *) &proc, sizeof(int), key, strlen(key)+1);
  }
}
