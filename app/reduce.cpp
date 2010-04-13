#include "stdlib.h"
#include "string.h"
#include "reduce.h"

using namespace APP_NS;

/* ---------------------------------------------------------------------- */

Reduce::Reduce(APP *app, char *idstr) : Pointers(app)
{
  int n = strlen(idstr) + 1;
  id = new char[n];
  strcpy(id,idstr);

  appreduce = NULL;
  appptr = NULL;
}

/* ---------------------------------------------------------------------- */

Reduce::~Reduce()
{
  delete [] id;
}
