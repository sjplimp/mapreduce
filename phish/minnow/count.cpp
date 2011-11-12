// count instances of strings

#include "phish.h"

#include <string>
#include <map>

using namespace std;

void count(int);
void sort();

/* ---------------------------------------------------------------------- */

typedef map<string, int> Hash;
Hash hash;

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init(&narg,&args);
  phish_input(0,count,sort,1);
  phish_output(0);
  phish_check();

  if (narg != 0) phish_error("Count syntax: count");

  phish_loop();
  phish_exit();
}

/* ---------------------------------------------------------------------- */

void count(int nvalues)
{
  char *buf;
  int len;

  if (nvalues != 1) phish_error("Count processes one-value datums");
  int type = phish_unpack(&buf,&len);
  if (type != PHISH_STRING) phish_error("Count processes string values");
  string str(buf,len);
  ++hash[str];
}

/* ---------------------------------------------------------------------- */

void sort()
{
  Hash::const_iterator end = hash.end(); 
  for (Hash::const_iterator i = hash.begin(); i != end; i++) {
    phish_pack_int(i->second);
    phish_pack_string((char *) i->first.c_str());
    phish_send(0);
  }
}
