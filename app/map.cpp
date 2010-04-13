#include "stdlib.h"
#include "string.h"
#include "map.h"

using namespace APP_NS;

/* ---------------------------------------------------------------------- */

Map::Map(APP *app, char *idstr) : Pointers(app)
{
  int n = strlen(idstr) + 1;
  id = new char[n];
  strcpy(id,idstr);

  appmap = NULL;
  appmap_file_list = NULL;
  appmap_file = NULL;
  appmap_mr = NULL;
  appptr = NULL;
}

/* ---------------------------------------------------------------------- */

Map::~Map()
{
  delete [] id;
}
