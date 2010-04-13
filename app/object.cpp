#include "string.h"
#include "stdlib.h"
#include "object.h"
#include "mrmpi.h"
#include "map.h"
#include "reduce.h"
#include "hash.h"
#include "compare.h"
#include "style_map.h"
#include "style_reduce.h"
#include "style_hash.h"
#include "style_compare.h"
#include "memory.h"
#include "error.h"

#include "mapreduce.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;

#define DELTA 16

enum{MAPREDUCE,MAP,REDUCE,HASH,COMPARE};      // same as elsewhere

/* ---------------------------------------------------------------------- */

Object::Object(APP *app) : Pointers(app)
{
  nobj = maxobj = 0;
  objs = NULL;

  nclass = 5;
  classes = new char*[5];
  classes[0] = "mrmpi";
  classes[1] = "map";
  classes[2] = "reduce";
  classes[3] = "hash";
  classes[4] = "compare";

  mrmpi = new MRMPI(app);
}

/* ---------------------------------------------------------------------- */

Object::~Object()
{
  for (int i = 0; i < nobj; i++) {
    delete [] objs[i].id;
    if (objs[i].type == MAPREDUCE) delete (MapReduce *) objs[i].ptr;
    else if (objs[i].type == MAP) delete (Map *) objs[i].ptr;
    else if (objs[i].type == REDUCE) delete (Reduce *) objs[i].ptr;
    else if (objs[i].type == HASH) delete (Hash *) objs[i].ptr;
    else if (objs[i].type == COMPARE) delete (Compare *) objs[i].ptr;
  }
  memory->sfree(objs);
  delete [] classes;
  delete mrmpi;
}

/* ----------------------------------------------------------------------
   classify a name as either a class of objects or an instantiated object
   return 1 if name of a class
   return 2 if name of a specific objects
   return 0 if unrecognized as either
   also set index for which class or object it is
------------------------------------------------------------------------- */

int Object::classify_object(char *name, int &index)
{
  for (index = 0; index < nclass; index++)
    if (strcmp(classes[index],name) == 0) return 1;
  for (index = 0; index < nobj; index++)
    if (strcmp(objs[index].id,name) == 0) return 2;
  return 0;
}

/* ----------------------------------------------------------------------
   find an object by name of type
   type >= 0 to match specific type
   type = -1 to match any type 
   return the objects ptr if found
   return NULL if not found
------------------------------------------------------------------------- */

void *Object::find_object(char *name, int type)
{
  for (int i = 0; i < nobj; i++) {
    if (type >= 0 && objs[i].type != type) continue;
    if (strcmp(objs[i].id,name) == 0) return objs[i].ptr;
  }
  return NULL;
}

/* ----------------------------------------------------------------------
   return variable value associated with a named object
   assume object exists
   return 0 if variable can be evaluated
   return 1 if it cannot
------------------------------------------------------------------------- */

int Object::variable_object(char *name, char *variable, double &value)
{
  int index;
  for (index = 0; index < nobj; index++)
    if (strcmp(objs[index].id,name) == 0) break;

  int flag;
  if (objs[index].type == MAPREDUCE)
    flag = mrmpi->variable(objs[index].ptr,variable,value);
  else {
    error->all("Object does not support variable evaluation");
  }

  return flag;
}

/* ---------------------------------------------------------------------- */

void Object::add_object(char *name, int type, void *ptr)
{
  if (find_object(name,-1)) error->all("Object name already exists");

  // grow object list if needed

  if (nobj == maxobj) {
    maxobj += DELTA;
    objs = (Obj *) memory->srealloc(objs,maxobj*sizeof(Obj),"Object:objs");
  }
  
  // store new object ID
  
  int n = strlen(name) + 1;
  objs[nobj].id = new char[n];
  strcpy(objs[nobj].id,name);
  objs[nobj].type = type;
  objs[nobj].ptr = ptr;
  nobj++;
}


/* ---------------------------------------------------------------------- */

void Object::create_object(int index, int narg, char **arg)
{
  if (narg < 2) error->all("Invalid command for creating an object");
  if (arg[0] == NULL) error->all("No object ID for object creation");

  void *ptr;

  if (index == MAPREDUCE) {
    if (narg > 2) error->all("Invalid command for creating an object");
    ptr = (void *) new MapReduce();
  } else if (index == MAP) {
    ptr = add_map(arg[1],narg-2,&arg[2]);
  } else if (index == REDUCE) {
    ptr = add_reduce(arg[1],narg-2,&arg[2]);
  } else if (index == HASH) {
    ptr = add_hash(arg[1],narg-2,&arg[2]);
  } else if (index == COMPARE) {
    ptr = add_compare(arg[1],narg-2,&arg[2]);
  }

  add_object(arg[0],index,ptr);
}

/* ---------------------------------------------------------------------- */

void Object::invoke_object(int index, int narg, char **arg)
{
  if (narg < 2) error->all("Invalid command for invoking an object");
  if (arg[1] == NULL) error->all("Invalid command for invoking an object");

  // if 2nd arg is delete, delete object here
  // compress object list by copying last object to index

  if (strcmp(arg[1],"delete") == 0) {
    if (narg > 2) error->all("Invalid command for deleting an object");
    if (arg[0]) error->all("Invalid command for deleting an object");
    if (objs[index].type == MAPREDUCE) delete (MapReduce *) objs[index].ptr;
    else if (objs[index].type == MAP) delete (Map *) objs[index].ptr;
    else if (objs[index].type == REDUCE) delete (Reduce *) objs[index].ptr;
    else if (objs[index].type == HASH) delete (Hash *) objs[index].ptr;
    else if (objs[index].type == COMPARE) delete (Compare *) objs[index].ptr;

    delete [] objs[index].id;
    if (index < nobj-1) {
      objs[index].id = objs[nobj-1].id;
      objs[index].type = objs[nobj-1].type;
      objs[index].ptr = objs[nobj-1].ptr;
    }
    nobj--;
    return;
  }

  // pass command to object for invocation

  void *ptr;

  if (objs[index].type == MAPREDUCE)
    ptr = mrmpi->command(objs[index].ptr,arg[1],narg-2,&arg[2]);
  else {
    error->all("Object does not support being invoked");
  }

  // new object is created as result of command

  if (ptr) {
    if (arg[0] == NULL) error->all("No object ID for invoking an object");
    add_object(arg[0],objs[index].type,ptr);
  } else if (arg[0])
    error->all("Invoking object does not create a new object");
}

/* ---------------------------------------------------------------------- */

void *Object::add_map(char *name, int narg, char **arg)
{
  void *ptr = NULL;

  if (0) return ptr;         // dummy line to enable else-if macro expansion

#define MAP_CLASS
#define MapStyle(key,Class) \
  else if (strcmp(name,#key) == 0) \
    ptr = (void *) new Class(app,name,narg,arg);
#include "style_map.h"
#undef MAP_CLASS

  else error->all("Invalid map style");

  return ptr;
}

/* ---------------------------------------------------------------------- */

void *Object::add_reduce(char *name, int narg, char **arg)
{
  void *ptr = NULL;

  if (0) return ptr;         // dummy line to enable else-if macro expansion

#define REDUCE_CLASS
#define ReduceStyle(key,Class) \
  else if (strcmp(name,#key) == 0) \
    ptr = (void *) new Class(app,name,narg,arg);
#include "style_reduce.h"
#undef REDUCE_CLASS

  else error->all("Invalid reduce style");

  return ptr;
}

/* ---------------------------------------------------------------------- */

void *Object::add_hash(char *name, int narg, char **arg)
{
  void *ptr = NULL;

  if (0) return ptr;         // dummy line to enable else-if macro expansion

#define HASH_CLASS
#define HashStyle(key,Class) \
  else if (strcmp(name,#key) == 0) \
    ptr = (void *) new Class(app,name,narg,arg);
#include "style_hash.h"
#undef HASH_CLASS

  else error->all("Invalid hash style");

  return ptr;
}

/* ---------------------------------------------------------------------- */

void *Object::add_compare(char *name, int narg, char **arg)
{
  void *ptr = NULL;

  if (0) return ptr;         // dummy line to enable else-if macro expansion

#define COMPARE_CLASS
#define CompareStyle(key,Class) \
  else if (strcmp(name,#key) == 0) \
    ptr = (void *) new Class(app,name,narg,arg);
#include "style_compare.h"
#undef COMPARE_CLASS

  else error->all("Invalid compare style");

  return ptr;
}
