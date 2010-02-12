// MapReduce word frequency example in C++
//  Syntax: wordfreq [-b] [-c] [-p] file1 file2 ...
//      or  wordfreq [-b] [-c] [-p] [-f {tpw,equiv,def}] -n #\n");
//  where
//      -b ==> redistribute words to processors before doing wordcount
//             (can help load balance when reading from files or doing 
//              non-default generation)
//      -c ==> locally compress data before doing global word count.
//      -p ==> generate post-processing histograms
//      -f {tpw,equiv,def} ==> type of generation to do;
//             tpw = task per word = generate only one word's occurrences in a 
//                                   single map task
//             equiv = file equivalent = N+1 tasks with words generated
//                                   equivalent to reading N+1 files.
//             def = default = equal number of words per processor generated.
//      -n # ==> generate (#+1)*2^# words.
//
// (1) reads all files, parses into words separated by whitespace 
//     or generates Eric Goodman's input of (2**(N+1)-1) words (-n option).
// (2) counts occurrence of each word in all files
// (3) prints top 10 words
//
// Based on wordfreq.cpp in the example directory.  Modified for experiments
// with out-of-core MapReduce.
//
// Limitations:  each word can occur no more than 2^31-1 times without 
// causing integer overflow in functions sum and globalsum.

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "sys/stat.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "blockmacros.hpp"
#include "genPowerLaw.hpp"

using namespace MAPREDUCE_NS;

typedef void MAPFN(int, KeyValue*, void*);
typedef void MAPFN2(int, char *, int, char *, int, KeyValue *, void *);
typedef void REDUCEFN(char*, int, char*, int, int*, KeyValue*, void*);
MAPFN fileread;
MAPFN fileread_redistrib;
MAPFN2 output;
REDUCEFN sum;
REDUCEFN globalsum;
REDUCEFN sanitycheck;
int ncompare(char *, int, char *, int);

struct Count {
  int n,limit,flag;
};

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);
  greetings();

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  if (me == 0) {
    for (int i=0; i < narg; i++) printf("%s ", args[i]);
    printf("\n");
    fflush(stdout);
  }

  bool readfiles = true;
  bool redistribute_flag = false;
  bool local_compress = false;
  bool postprocess = false;
  enum GenPowerLawEnum gen_flag = GEN_DEFAULT;
  int N = -1;
  int memsize = 2000;
  const char *optstring = "bcf:m:n:p";

  char ch;
  while ((ch = getopt(narg, args, optstring)) != -1) {
    switch (ch) {
    case 'b':
      redistribute_flag = true;
      break;
    case 'c':
      local_compress = true;
      break;
    case 'f':
      readfiles = false;
      if (strcmp(optarg, "tpw") == 0) 
        gen_flag = GEN_TASKPERWORD;
      else if (strcmp(optarg, "equiv") == 0)
        gen_flag = GEN_FILEEQUIV;
      else if (strcmp(optarg, "def") != 0) {
        printf("Invalid generation option %s\n", optarg);
        MPI_Abort(MPI_COMM_WORLD, -1);
      }
      break;
    case 'm':
      memsize = atoi(optarg);
      break;
    case 'n':
      readfiles = false;
      N = atoi(optarg);
      break;
    case 'p':
      postprocess = true;
      break;
    case '?':
      printf("Invalid option -%c\n", optopt);
      MPI_Abort(MPI_COMM_WORLD, -1);
      break;
    }
  }

  if (!readfiles && N < 0) {
    if (me == 0) printf("Error:  Must specify -n # to generate numbers\n");
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  if ((narg < 2) || (readfiles && (narg-optind) < 2)) {
    if (me == 0) 
      printf("Syntax: wordfreq [-b] [-c] [-p] [-m memsize] file1 file2 ..."
             "\nor\n"
             "        wordfreq [-b] [-c] [-p] [-m memsize] [-f={tpw,equiv,def}] -n #\n");
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
// mr->verbosity = 1;
// mr->timer = 1;
#ifdef NEW_OUT_OF_CORE
  mr->set_fpath(MYLOCALDISK);
  mr->memsize=memsize;
#endif

  MAPFN *genmap;
  int nmaptasks;
  void *mapptr;
  if (readfiles) {
    mapptr = &args[optind];
    nmaptasks = narg-optind;
    if (redistribute_flag) genmap = &fileread_redistrib;
    else genmap = &fileread;
    if (me == 0) 
      printf("Input:  FileRead Redistribute %d\n", redistribute_flag);
  } else {
    switch (gen_flag) {
    case GEN_TASKPERWORD:
      mapptr = &N;
      nmaptasks = (1<<(N+1))-1; 
      if (redistribute_flag) genmap = genPowerLaw_TaskPerWord_Redistrib;
      else genmap = genPowerLaw_TaskPerWord;
      if (me == 0) 
        printf("Input:  Generate TaskPerWord  Redistribute %d\n", 
               redistribute_flag);
      break;
    case GEN_FILEEQUIV:
      nmaptasks = N+1;
      mapptr = &N;
      if (redistribute_flag) genmap = genPowerLaw_FileEquiv_Redistrib;
      else genmap = genPowerLaw_FileEquiv;
      if (me == 0) 
        printf("Input:  Generate FileEquiv  Redistribute %d\n", 
               redistribute_flag);
      break;
    default:
      mapptr = &N;
      nmaptasks = nprocs;
      if (redistribute_flag) genmap = genPowerLaw_Redistrib;
      else genmap = genPowerLaw;
      if (me == 0) 
        printf("Input:  Generate Default  Redistribute %d\n", 
               redistribute_flag);
      break;
    }
  }

  if (me == 0) {printf("Begin map...\n"); fflush(stdout);}
  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  // Generate words and (optionally) redistribute them to processors.
  uint64_t nwords;
  nwords = mr->map(nmaptasks,genmap,mapptr);
  if (redistribute_flag) {
    if (me == 0) {printf("Begin redistribution...collate\n"); fflush(stdout);}
    mr->collate(&genPowerLaw_IdentityHash);  // Collates by processor
    if (me == 0) {printf("                    ...reduce\n"); fflush(stdout);}
    mr->reduce(&genPowerLaw_RedistributeReduce, NULL);
  }

#ifdef NEW_OUT_OF_CORE
  mr->cummulative_stats(2, 1);
#endif

  MPI_Barrier(MPI_COMM_WORLD);
  double tread = MPI_Wtime();

  if (me == 0) {printf("Begin wordcount...\n"); fflush(stdout);}

  uint64_t nunique;
  if (local_compress) {
    // Perform a local compression of the data first to reduce the amount
    // of communication that needs to be done.
    // This compression builds local hash tables, and requires an additional
    // pass over the data..
    if (me == 0) {printf("               ...compress\n"); fflush(stdout);}
    nunique = mr->compress(&sum,NULL);  // Do word-count locally first
    if (nprocs > 1) {
      mr->collate(NULL);        // Hash keys and local counts to processors
      if (me == 0) {printf("               ...reduce\n"); fflush(stdout);}
      nunique = mr->reduce(&globalsum,NULL); // Compute global sums for each key
    }
  }
  else {
    // Do not do a local compression but, rather, do a global aggregate only.
    mr->collate(NULL);    
    if (me == 0) {printf("               ...reduce\n"); fflush(stdout);}
    nunique = mr->reduce(&sum,NULL);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

#ifdef NEW_OUT_OF_CORE
  mr->cummulative_stats(2, 0);
#endif

  if (postprocess) {
    if (me == 0) {printf("Begin post-process...copy\n"); fflush(stdout);}
#ifdef NEW_OUT_OF_CORE
    MapReduce *mrout = mr->copy();
#else
    MapReduce *mrout = new MapReduce(*mr);
#endif
    if (me == 0) {printf("                  ...sort_values\n");fflush(stdout);}
    mrout->sort_values(&ncompare);

    Count count;
    count.n = 0;
    count.limit = 10;
    count.flag = 0;
    if (me == 0) {printf("                  ...map\n");fflush(stdout);}
#ifdef NEW_OUT_OF_CORE
    mrout->map(mrout, &output, &count);
#else
    mrout->map(mrout->kv, &output, &count);
#endif
  
    if (me == 0) {printf("                  ...gather\n");fflush(stdout);}
    mrout->gather(1);
    if (me == 0) {printf("                  ...sort_values\n");fflush(stdout);}
    mrout->sort_values(&ncompare);
  
    count.n = 0;
    count.limit = 10;
    count.flag = 1;
    if (me == 0) {printf("                  ...map\n");fflush(stdout);}
#ifdef NEW_OUT_OF_CORE
    mrout->map(mrout, &output, &count);
#else
    mrout->map(mrout->kv, &output, &count);
#endif

    delete mrout;
  }  // if postprocess


#define SANITY_TEST
#ifdef SANITY_TEST
  if (me == 0) {printf("Begin sanity check...clone\n"); fflush(stdout);}
  mr->clone();
  if (me == 0) {printf("                  ...reduce\n"); fflush(stdout);}

  if (readfiles) {
    // Check that the sum of the word counts for all words = nwords.
    // mr contains key = word, value = occurrence count for word.
    // Compute sum of occurrence counts.
    uint64_t mynwords = 0, gnwords;
    mr->reduce(&sanitycheck, (void *) &mynwords);
    MPI_Allreduce(&mynwords, &gnwords, 1, MPI_UNSIGNED_LONG, 
                  MPI_SUM, MPI_COMM_WORLD);
  if (me == 0)
    if (nwords != gnwords) 
      printf("SANITY TEST FAILED:  nwords = %llu  sum of counts = %llu\n",
             nwords, gnwords);
    else
      printf("Sanity test OK.\n");
  }
  else {
    // We generated the words using the PowerLaw distribution, so
    // we can check the accuracy of each word's count.
    uint64_t gerrors = 0;
    genPowerLaw_SanityStruct a(N);
    mr->reduce(&genPowerLaw_SanityTest, (void *) &a);
    MPI_Allreduce(&(a.nerrors), &gerrors, 1, MPI_UNSIGNED_LONG, 
                  MPI_SUM, MPI_COMM_WORLD);
    if (me == 0)
      if (gerrors > 0) printf("SANITY TEST FAILED:  %llu ERRORS\n", gerrors);
      else printf("Sanity test OK.\n");
  }
#endif

  delete mr;

  MPI_Barrier(MPI_COMM_WORLD);
  double tpost = MPI_Wtime();

  if (me == 0) {
    printf("%llu total words, %llu unique words\n",nwords,nunique);
    if (readfiles)
      printf("Time for fileread:  %g (secs)\n", tread-tstart);
    else 
      printf("Time for genwords:  %g (secs)\n", tread-tstart);
    printf("Time for wordcount: %g (secs)\n", tstop-tread);
    if (readfiles)
      printf("Total Time to process %d files on %d procs = %g (secs)\n",
             narg-1,nprocs,tstop-tstart);
    else
      printf("Total Time to process with N=%d on %d procs = %g (secs)\n",
             N,nprocs,tstop-tstart);
    printf("Time for post-processing:  %g (secs)\n", tpost-tstop);
  }

  MPI_Finalize();
}

/* ----------------------------------------------------------------------
   read a file
   for each word in file, emit key = word, value = NULL
------------------------------------------------------------------------- */

void fileread(int itask, KeyValue *kv, void *ptr)
{
  // filesize = # of bytes in file

  char **files = (char **) ptr;

  struct stat stbuf;
  int flag = stat(files[itask],&stbuf);
  if (flag < 0) {
    printf("ERROR: Could not query file size\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
  int filesize = stbuf.st_size;

  FILE *fp = fopen(files[itask],"r");
  char *text = new char[filesize+1];
  int nchar = fread(text,1,filesize,fp);
  text[nchar] = '\0';
  fclose(fp);

  const char *whitespace = " \t\n\f\r\0";
  char *word = strtok(text,whitespace);
  while (word) {
    kv->add(word,strlen(word)+1,NULL,0);
    word = strtok(NULL,whitespace);
  }

  delete [] text;
}

/* ----------------------------------------------------------------------
   read a file
   for each word in file, emit key = processor, value = word
------------------------------------------------------------------------- */

void fileread_redistrib(int itask, KeyValue *kv, void *ptr)
{
  // filesize = # of bytes in file

  char **files = (char **) ptr;

  struct stat stbuf;
  int flag = stat(files[itask],&stbuf);
  if (flag < 0) {
    printf("ERROR: Could not query file size\n");
    MPI_Abort(MPI_COMM_WORLD,1);
  }
  int filesize = stbuf.st_size;

  FILE *fp = fopen(files[itask],"r");
  char *text = new char[filesize+1];
  int nchar = fread(text,1,filesize,fp);
  text[nchar] = '\0';
  fclose(fp);

  static int count = 0;
  static int np;
  if (count == 0) MPI_Comm_size(MPI_COMM_WORLD, &np);
  const char *whitespace = " \t\n\f\r\0";
  char *word = strtok(text,whitespace);
  while (word) {
    int proc = count % np;
    count++;
    kv->add((char *) &proc, sizeof(int), word,strlen(word)+1);
    word = strtok(NULL,whitespace);
  }

  delete [] text;
}

/* ----------------------------------------------------------------------
   count word occurrence
   emit key = word, value = # of multi-values
------------------------------------------------------------------------- */

void sum(char *key, int keybytes, char *multivalue,
         int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  uint64_t total_nvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, total_nvalues)
  kv->add(key,keybytes,(char *) &total_nvalues,sizeof(uint64_t));
}

/* ----------------------------------------------------------------------
   count word occurrence
   emit key = word, value = sum of multi-values
------------------------------------------------------------------------- */

void globalsum(char *key, int keybytes, char *multivalue,
         int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  uint64_t sum = 0; 
  uint64_t total_nvalues;
  CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, total_nvalues)
  BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)

  for (int i = 0; i < nvalues; i++) sum += *(((uint64_t *) multivalue)+i);

  END_BLOCK_LOOP

  kv->add(key,keybytes,(char *) &sum,sizeof(uint64_t));
}

/* ----------------------------------------------------------------------
   compare two counts
   order values by count, largest first
------------------------------------------------------------------------- */

int ncompare(char *p1, int len1, char *p2, int len2)
{
  uint64_t i1 = *(uint64_t *) p1;
  uint64_t i2 = *(uint64_t *) p2;
  if (i1 > i2) return -1;
  else if (i1 < i2) return 1;
  else return 0;
}

/* ----------------------------------------------------------------------
   process a word and its count
   depending on flag, emit KV or print it, up to limit
------------------------------------------------------------------------- */

void output(int itask, char *key, int keybytes, char *value,
            int valuebytes, KeyValue *kv, void *ptr)
{
  Count *count = (Count *) ptr;
  count->n++;
  if (count->n > count->limit) return;

  uint64_t n = *(uint64_t *) value;
  if (count->flag) printf("%llu %s\n",n,key);
  else kv->add(key,keybytes,(char *) &n,sizeof(uint64_t));
}

/* ----------------------------------------------------------------------
   For a sanity check, ensure that the sum of the wordcounts equals the
   total number of words.  Compute the sum of the wordcounts on this 
   processor.
   Input:  KMV:  key = word, nvalues = 1, mvalue = wordcount for word
   Output:  NONE
   Side effect:  increment subtotal by mvalue.
------------------------------------------------------------------------- */

void sanitycheck(char *key, int keybytes, char *multivalue,
                 int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  uint64_t *subtotal = (uint64_t *) ptr;
  *subtotal += *((uint64_t *) multivalue);
}
