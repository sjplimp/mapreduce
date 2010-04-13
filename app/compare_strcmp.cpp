#include "string.h"
#include "compare_strcmp.h"
#include "error.h"

using namespace APP_NS;

/* ---------------------------------------------------------------------- */

CompareStrCmp::CompareStrCmp(APP *app, char *idstr, int narg, char **arg) : 
  Compare(app, idstr)
{
  if (narg) error->all("Invalid compare strcmp args");

  appcompare = compare;
}

/* ----------------------------------------------------------------------
   compare two strings, assume terminated by NULL byte
------------------------------------------------------------------------- */

int CompareStrCmp::compare(char *p1, int len1, char *p2, int len2)
{
  return strcmp(p1,p2);
}
