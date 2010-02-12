// A baseline program for doing word counting serially without anything 
// special.  Uses STL extension hash_map for storing unique words.
// With -n option, we'll generate the data sets as in wordfreq.
// Without option, we'll read files off the command line.  E.g., 
//    a.out file1 file2 file3
//    a.out -n 12

#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "sys/stat.h"
#include <mpi.h>
#include <string>
#include <ext/hash_map>

using namespace std;
using namespace __gnu_cxx;

#define BUFLEN 1000000

namespace __gnu_cxx
{
  template<> struct hash< std::string >
  {
    size_t operator()( const std::string& x ) const
    {
      return hash< const char* >()( x.c_str() );
    }
  };
}

void check_word(string &word, uint64_t *wordcount, uint64_t *uniquewordcount) 
{
  static hash_map<string, int> tab;
  static hash_map<string, int>::iterator tabiter;

  (*wordcount)++;
  if ((tabiter = tab.find(word)) == tab.end()) {
    // Unique word; count it and add to table.
    tab[word] = 1;
    (*uniquewordcount)++;
  }
  else {
    // Not a unique word; increment its count.
    (tabiter->second)++;
  }
}


int main(int narg, char *arg[])
{
bool generate_words = false;
int word_dim;
uint64_t wordcount = 0;
uint64_t uniquewordcount = 0;
 
  MPI_Init(&narg, &arg);

  if (narg == 1) {
    printf("Usage:  %s filename1 filename2 ...\n"
           "or\n %s -n # \n", arg[0], arg[0]);
    exit(-1);
  }

  if (strcmp(arg[1], "-n") == 0) {
    generate_words = true;
    word_dim = atoi(arg[2]);
  }

  double tstart = MPI_Wtime();

  if (generate_words) {
    // Berry's power-law numbers word-count test.
    for (int i = 0; i <= word_dim; i++) {
      uint64_t nrepeat = (1 << (word_dim - i));
      uint64_t first = (1 << i) - 1;
      uint64_t last = (1 << (i+1)) - 2;
      for (uint64_t j = first; j <= last; j++) {
        char chword[256];
        sprintf(chword, "%ld", j);
        string word(chword);
        for (uint64_t m = 0; m < nrepeat; m++) {
          check_word(word, &wordcount, &uniquewordcount);
        }
      }
    }
  }
  else {
    for (int f = 1; f < narg; f++) {
      FILE *fp = fopen(arg[f], "r");
      char chword[256];
      char ch = getc(fp);
      int i = 0;
      while (ch != EOF) {
        while (ch != ' ' && ch != '\n') {
          chword[i++] = ch;
          ch = getc(fp);
        }
        chword[i] = '\0';

        string word(chword);
        check_word(word, &wordcount, &uniquewordcount);

        i = 0;
        ch = getc(fp);
      }
    }
  }
  double tend = MPI_Wtime();

  printf("%ld total words, %ld unique words, %f seconds\n", 
         wordcount, uniquewordcount, tend - tstart);
  MPI_Finalize();
}
