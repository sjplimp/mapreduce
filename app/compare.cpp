#include "stdlib.h"
#include "string.h"
#include "compare.h"

using namespace APP_NS;

/* ---------------------------------------------------------------------- */

Compare::Compare(APP *app, char *idstr) : Pointers(app)
{
  int n = strlen(idstr) + 1;
  id = new char[n];
  strcpy(id,idstr);

  appcompare = NULL;
}

/* ---------------------------------------------------------------------- */

Compare::~Compare()
{
  delete [] id;
}
