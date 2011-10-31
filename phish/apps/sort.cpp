// sort datums, emit top ones

#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "phish.h"

#include <functional>
#include <utility>
#include <string>
#include <vector>
#include <algorithm>

using namespace std;

void store(int);
void process();

/* ---------------------------------------------------------------------- */

vector< pair<int,string> > list;
int ntop;

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init("sort",1,1,&narg,&args);
  phish_callback_datum(store);
  phish_callback_done(process);

  if (narg != 1) phish_error("Sort syntax: sort N");
  ntop = atoi(args[0]);

  phish_loop();
  phish_close();
}

/* ---------------------------------------------------------------------- */

void store(int nvalues)
{
  char *count,*word;
  int len;

  if (nvalues != 2) phish_error("Sort processes two-value datums");
  int type1 = phish_unpack_next(&count,&len);
  int type2 = phish_unpack_next(&word,&len);
  if (type1 != PHISH_INT) phish_error("Sort processes int/string datums");

  string str(word,strlen(word));
  list.push_back(make_pair(*(int *) count,str));
}

/* ---------------------------------------------------------------------- */

void process()
{
  sort(list.begin(),list.end(),greater< pair<int,string> >());

  for (int i = 0; i < ntop; i++) {
    phish_pack_int(list[i].first);
    phish_pack_string((char *) list[i].second.c_str());
    phish_send();
  }

  phish_send_done();
}
