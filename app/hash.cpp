#include "stdlib.h"
#include "string.h"
#include "hash.h"

using namespace APP_NS;

/* ---------------------------------------------------------------------- */

Hash::Hash(APP *app, char *idstr) : Pointers(app)
{
  int n = strlen(idstr) + 1;
  id = new char[n];
  strcpy(id,idstr);

  apphash = NULL;
}

/* ---------------------------------------------------------------------- */

Hash::~Hash()
{
  delete [] id;
}
