#include "hash_simple.h"
#include "error.h"

using namespace APP_NS;

/* ---------------------------------------------------------------------- */

HashSimple::HashSimple(APP *app, char *idstr, int narg, char **arg) :
  Hash(app, idstr)
{
  if (narg) error->all("Invalid hash simple args");

  apphash = hash;
}

/* ---------------------------------------------------------------------- */

int HashSimple::hash(char *str, int len)
{
  return 0;
}
