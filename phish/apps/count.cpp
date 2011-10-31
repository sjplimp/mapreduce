// count instances of keyed datums

#include "mpi.h"
#include "phish.h"

#include <string>
#include <map>

using namespace std;

void count(int);
void done();

/* ---------------------------------------------------------------------- */

typedef map<string, int> Hash;
Hash hash;

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init("count",1,1,&narg,&args);
  phish_callback_datum(count);
  phish_callback_done(done);

  if (narg != 0) phish_error("Count syntax: count");

  phish_loop();
  phish_close();
}

/* ---------------------------------------------------------------------- */

void count(int nvalues)
{
  char *buf;
  int len;

  if (nvalues != 1) phish_error("Count processes one-value datums");
  int type = phish_unpack_next(&buf,&len);
  if (type != PHISH_STRING) phish_error("Count processes string values");
  string str(buf,len);
  ++hash[str];
}

/* ---------------------------------------------------------------------- */

void done()
{
  Hash::const_iterator end = hash.end(); 
  for (Hash::const_iterator i = hash.begin(); i != end; i++) {
    phish_pack_int(i->second);
    phish_pack_string((char *) i->first.c_str());
    phish_send();
  }

  phish_send_done();
}
