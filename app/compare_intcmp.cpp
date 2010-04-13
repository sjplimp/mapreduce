#include "compare_intcmp.h"
#include "error.h"

using namespace APP_NS;

/* ---------------------------------------------------------------------- */

CompareIntCmp::CompareIntCmp(APP *app, char *idstr, int narg, char **arg) : 
  Compare(app, idstr)
{
  if (narg) error->all("Invalid compare intcmp args");

  appcompare = compare;
}

/* ----------------------------------------------------------------------
   compare two ints
------------------------------------------------------------------------- */

int CompareIntCmp::compare(char *p1, int len1, char *p2, int len2)
{
  int i1 = *(int *) p1;
  int i2 = *(int *) p2;
  if (i1 > i2) return -1;
  else if (i1 < i2) return 1;
  else return 0;
}
