#include "string.h"
#include "stdlib.h"
#include "ctype.h"
#include "mrmpi.h"
#include "object.h"
#include "map.h"
#include "reduce.h"
#include "hash.h"
#include "compare.h"
#include "memory.h"
#include "error.h"

#include "mapreduce.h"
#include "keyvalue.h"
#include "keymultivalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;

enum{MAPREDUCE,MAP,REDUCE,HASH,COMPARE};   // same as in object.cpp

/* ---------------------------------------------------------------------- */

MRMPI::MRMPI(APP *app) : Pointers(app) {}

/* ----------------------------------------------------------------------
   invoke a command associated with mrptr
   return NULL if command does not generate a new MRMPI
   else return ptr to new MRMPI
------------------------------------------------------------------------- */

void *MRMPI::command(void *mrptr, char *command, int narg, char **arg)
{
  mr = (MapReduce *) mrptr;

  if (!strcmp(command,"copy")) {
    MapReduce *mr2 = copy_command(narg,arg);
    return (void *) mr2;
  }

  int flag = 0;

  if (!strcmp(command,"add")) add(narg,arg);
  else if (!strcmp(command,"aggregate")) aggregate(narg,arg);
  else if (!strcmp(command,"clone")) clone(narg,arg);
  else if (!strcmp(command,"collapse")) collapse(narg,arg);
  else if (!strcmp(command,"collate")) collate(narg,arg);
  else if (!strcmp(command,"compress")) compress(narg,arg);
  else if (!strcmp(command,"convert")) convert(narg,arg);
  else if (!strcmp(command,"gather")) gather(narg,arg);
  else if (!strcmp(command,"map")) map(narg,arg);
  else if (!strcmp(command,"map_file_list")) map_file_list(narg,arg);
  else if (!strcmp(command,"map_file_char")) map_file_char(narg,arg);
  else if (!strcmp(command,"map_file_str")) map_file_str(narg,arg);
  else if (!strcmp(command,"map_mr")) map_mr(narg,arg);
  else if (!strcmp(command,"reduce")) reduce(narg,arg);
  else if (!strcmp(command,"scrunch")) scrunch(narg,arg);
  else if (!strcmp(command,"sort_keys")) sort_keys(narg,arg);
  else if (!strcmp(command,"sort_values")) sort_values(narg,arg);
  else if (!strcmp(command,"sort_multivalues")) sort_multivalues(narg,arg);
  else if (!strcmp(command,"kv_stats")) kv_stats(narg,arg);
  else if (!strcmp(command,"kmv_stats")) kmv_stats(narg,arg);
  else if (!strcmp(command,"cummulative_stats")) cummulative_stats(narg,arg);
  else if (!strcmp(command,"print")) print(narg,arg);
  else flag = set(command,narg,arg);

  if (flag) {
    char str[128];
    sprintf(str,"Unrecognized MRMPI command: %s\n",command);
    error->all(str);
  }

  return NULL;
}

/* ----------------------------------------------------------------------
   return variable value associated with mrptr
   return 0 if variable can be evaluated
   return 1 if it cannot
------------------------------------------------------------------------- */

int MRMPI::variable(void *mrptr, char *variable, double &value)
{
  mr = (MapReduce *) mrptr;

  if (strcmp(variable,"nkv") == 0) {
    if (mr->kv == NULL)
      error->all("Requesting MRMPI nkv variable when KeyValue does not exist");
    value = mr->kv->nkv;
  } else if (strcmp(variable,"nkmv") == 0) {
    if (mr->kmv == NULL)
      error->all("Requesting MRMPI nkmv variable when KeyMultiValue "
		 "does not exist");
    value = mr->kmv->nkmv;
  } else return 1;

  return 0;
}

/* ---------------------------------------------------------------------- */

MapReduce *MRMPI::copy_command(int narg, char **arg)
{
  if (narg) error->all("Illegal MRMPI copy command");
  return mr->copy();
}

/* ---------------------------------------------------------------------- */

void MRMPI::add(int narg, char **arg)
{
  if (narg != 1) error->all("Illegal MRMPI add command");
  MapReduce *mr2 = (MapReduce *) obj->find_object(arg[0],MAPREDUCE);
  if (!mr2) error->all("Add argument is not an MRMPI");
  mr->add(mr2);
}

/* ---------------------------------------------------------------------- */

void MRMPI::aggregate(int narg, char **arg)
{
  if (narg > 1) error->all("Illegal MRMPI aggregate command");
  if (narg == 0) mr->aggregate(NULL);
  else {
    Hash *hash = (Hash *) obj->find_object(arg[0],HASH);
    if (!hash) error->all("Invalid aggregate apphash");
    mr->aggregate(hash->apphash);
  }
}

/* ---------------------------------------------------------------------- */

void MRMPI::clone(int narg, char **arg)
{
  if (narg) error->all("Illegal MRMPI clone command");
  mr->clone();
}

/* ---------------------------------------------------------------------- */

void MRMPI::collapse(int narg, char **arg)
{
  if (narg != 1) error->all("Illegal MRMPI collapse command");

  // if all key chars are digits or '-', then treat as int
  // else treat as char string

  int flag = 0;
  for (int i = 0; i < strlen(arg[0]); i++)
    if (!(isdigit(arg[0][i]) || arg[0][i] == '-')) flag = 1;

  if (flag == 0) {
    int key = atoi(arg[0]);
    mr->collapse((char *) &key,sizeof(int));
  } else {
    mr->collapse(arg[0],strlen(arg[0])+1);
  }
}

/* ---------------------------------------------------------------------- */

void MRMPI::collate(int narg, char **arg)
{
  if (narg > 1) error->all("Illegal MRMPI collate command");
  if (narg == 0) mr->collate(NULL);
  else {
    Hash *hash = (Hash *) obj->find_object(arg[0],HASH);
    if (!hash) error->all("Invalid collate apphash");
    mr->collate(hash->apphash);
  }
}

/* ---------------------------------------------------------------------- */

void MRMPI::compress(int narg, char **arg)
{
  if (narg != 1) error->all("Illegal MRMPI compress command");
  Reduce *reduce = (Reduce *) obj->find_object(arg[0],REDUCE);
  if (!reduce) error->all("Invalid compress appreduce");
  mr->compress(reduce->appreduce,reduce->appptr);
}

/* ---------------------------------------------------------------------- */

void MRMPI::convert(int narg, char **arg)
{
  if (narg) error->all("Illegal MRMPI convert command");
  mr->convert();
}

/* ---------------------------------------------------------------------- */

void MRMPI::gather(int narg, char **arg)
{
  if (narg != 1) error->all("Illegal MRMPI gather command");
  int nprocs = atoi(arg[0]);
  mr->gather(nprocs);
}

/* ---------------------------------------------------------------------- */

void MRMPI::map(int narg, char **arg)
{
  if (narg < 2 || narg > 3) error->all("Illegal MRMPI map command");
  int ntask = atoi(arg[0]);
  Map *map = (Map *) obj->find_object(arg[1],MAP);
  if (!map) error->all("Invalid map appmap");
  if (!map->appmap) error->all("Map appmap is not correct style");
  if (narg == 2)
    mr->map(ntask,map->appmap,map->appptr);
  else
    mr->map(ntask,map->appmap,map->appptr,atoi(arg[2]));
}

/* ---------------------------------------------------------------------- */

void MRMPI::map_file_list(int narg, char **arg)
{
  if (narg < 2 || narg > 3) error->all("Illegal MRMPI map_file_list command");
  Map *map = (Map *) obj->find_object(arg[1],MAP);
  if (!map) error->all("Invalid map appmap");
  if (!map->appmap_file_list) error->all("Map appmap is not correct style");
  if (narg == 2)
    mr->map(arg[0],map->appmap_file_list,map->appptr);
  else
    mr->map(arg[0],map->appmap_file_list,map->appptr,atoi(arg[2]));
}

/* ---------------------------------------------------------------------- */

void MRMPI::map_file_char(int narg, char **arg)
{
  if (narg < 6) error->all("Illegal MRMPI map_file_char command");
  int ntask = atoi(arg[0]);
  if (strlen(arg[1]) != 1)
    error->all("Map_file_char command does not have char arg");
  char sepchar = arg[1][0];
  int delta = atoi(arg[2]);
  Map *map = (Map *) obj->find_object(arg[3],MAP);
  if (!map) error->all("Invalid map appmap");
  if (!map->appmap_file) error->all("Map appmap is not correct style");
  int addflag = atoi(arg[4]);
  mr->map(ntask,narg-5,&arg[5],sepchar,delta,
	  map->appmap_file,map->appptr,addflag);
}

/* ---------------------------------------------------------------------- */

void MRMPI::map_file_str(int narg, char **arg)
{
  if (narg < 6) error->all("Illegal MRMPI map_file_str command");
  int ntask = atoi(arg[0]);
  char *sepstr = arg[1];
  int delta = atoi(arg[2]);
  Map *map = (Map *) obj->find_object(arg[3],MAP);
  if (!map) error->all("Invalid map appmap");
  if (!map->appmap_file) error->all("Map appmap is not correct style");
  int addflag = atoi(arg[4]);
  mr->map(ntask,narg-5,&arg[5],sepstr,delta,
	  map->appmap_file,map->appptr,addflag);
}

/* ---------------------------------------------------------------------- */

void MRMPI::map_mr(int narg, char **arg)
{
  if (narg < 2 || narg > 3) error->all("Illegal MRMPI map_mr command");
  MapReduce *mr2 = (MapReduce *) obj->find_object(arg[0],MAPREDUCE);
  if (!mr2) error->all("Map argument is not an MRMPI");
  Map *map = (Map *) obj->find_object(arg[1],MAP);
  if (!map) error->all("Invalid map appmap");
  if (!map->appmap_mr) error->all("Map appmap is not correct style");
  if (narg == 2)
    mr->map(mr2,map->appmap_mr,map->appptr);
  else
    mr->map(mr2,map->appmap_mr,map->appptr,atoi(arg[2]));
}

/* ---------------------------------------------------------------------- */

void MRMPI::reduce(int narg, char **arg)
{
  if (narg != 1) error->all("Illegal MRMPI reduce command");
  Reduce *reduce = (Reduce *) obj->find_object(arg[0],REDUCE);
  if (!reduce) error->all("Invalid reduce appreduce");
  mr->reduce(reduce->appreduce,reduce->appptr);
}

/* ---------------------------------------------------------------------- */

void MRMPI::scrunch(int narg, char **arg)
{
  if (narg != 2) error->all("Illegal MRMPI scrunch command");
  int nprocs = atoi(arg[0]);

  // if all key chars are digits or '-', then treat as int
  // else treat as char string

  int flag = 0;
  for (int i = 0; i < strlen(arg[1]); i++)
    if (!(isdigit(arg[1][i]) || arg[1][i] == '-')) flag = 1;

  if (flag == 0) {
    int key = atoi(arg[1]);
    mr->scrunch(nprocs,(char *) &key,sizeof(int));
  } else {
    mr->scrunch(nprocs,arg[1],strlen(arg[0])+1);
  }
}

/* ---------------------------------------------------------------------- */

void MRMPI::sort_keys(int narg, char **arg)
{
  if (narg != 1) error->all("Illegal MRMPI sort_keys command");
  Compare *compare = (Compare *) obj->find_object(arg[0],COMPARE);
  if (!compare) error->all("Invalid sort_keys appcompare");
  mr->sort_keys(compare->appcompare);
}

/* ---------------------------------------------------------------------- */

void MRMPI::sort_values(int narg, char **arg)
{
  if (narg != 1) error->all("Illegal MRMPI sort_values command");
  Compare *compare = (Compare *) obj->find_object(arg[0],COMPARE);
  if (!compare) error->all("Invalid sort_values appcompare");
  mr->sort_values(compare->appcompare);
}

/* ---------------------------------------------------------------------- */

void MRMPI::sort_multivalues(int narg, char **arg)
{
  if (narg != 1) error->all("Illegal MRMPI sort_multivalues command");
  Compare *compare = (Compare *) obj->find_object(arg[0],COMPARE);
  if (!compare) error->all("Invalid sort_multivalues appcompare");
  mr->sort_multivalues(compare->appcompare);
}

/* ---------------------------------------------------------------------- */

void MRMPI::kv_stats(int narg, char **arg)
{
  if (narg != 1) error->all("Illegal MRMPI kv_stats command");
  int level = atoi(arg[0]);
  mr->kv_stats(level);
}

/* ---------------------------------------------------------------------- */

void MRMPI::kmv_stats(int narg, char **arg)
{
  if (narg != 1) error->all("Illegal MRMPI kmv_stats command");
  int level = atoi(arg[0]);
  mr->kmv_stats(level);
}

/* ---------------------------------------------------------------------- */

void MRMPI::cummulative_stats(int narg, char **arg)
{
  if (narg != 2) error->all("Illegal MRMPI cummulative_stats command");
  int level = atoi(arg[0]);
  int reset = atoi(arg[1]);
  mr->cummulative_stats(level,reset);
}

/* ---------------------------------------------------------------------- */

void MRMPI::print(int narg, char **arg)
{
  if (narg != 4) error->all("Illegal MRMPI print command");
  int iproc = atoi(arg[0]);
  int nstride = atoi(arg[1]);
  int kflag = atoi(arg[2]);
  int vflag = atoi(arg[3]);
  mr->print(iproc,nstride,kflag,vflag);
}

/* ---------------------------------------------------------------------- */

int MRMPI::set(char *command, int narg, char **arg)
{
  int flag = 0;

  if (strcmp(command,"mapstyle") == 0) {
    if (narg != 1) error->all("Illegal MRMPI mapstyle command");
    mr->mapstyle = atoi(arg[0]);
  } else if (strcmp(command,"all2all") == 0) {
    if (narg != 1) error->all("Illegal MRMPI all2all command");
    mr->all2all = atoi(arg[0]);
  } else if (strcmp(command,"verbosity") == 0) {
    if (narg != 1) error->all("Illegal MRMPI verbosity command");
    mr->verbosity = atoi(arg[0]);
  } else if (strcmp(command,"timer") == 0) {
    if (narg != 1) error->all("Illegal MRMPI timer command");
    mr->timer = atoi(arg[0]);
  } else if (strcmp(command,"memsize") == 0) {
    if (narg != 1) error->all("Illegal MRMPI memsize command");
    mr->memsize = atoi(arg[0]);
  } else if (strcmp(command,"minpage") == 0) {
    if (narg != 1) error->all("Illegal MRMPI minpage command");
    mr->minpage = atoi(arg[0]);
  } else if (strcmp(command,"maxpage") == 0) {
    if (narg != 1) error->all("Illegal MRMPI maxpage command");
    mr->maxpage = atoi(arg[0]);
  } else if (strcmp(command,"keyalign") == 0) {
    if (narg != 1) error->all("Illegal MRMPI keyalign command");
    mr->keyalign = atoi(arg[0]);
  } else if (strcmp(command,"valuealign") == 0) {
    if (narg != 1) error->all("Illegal MRMPI valuealign command");
    mr->valuealign = atoi(arg[0]);
  } else if (strcmp(command,"fpath") == 0) {
    if (narg != 1) error->all("Illegal MRMPI fpath command");
    mr->set_fpath(arg[0]);
  } else flag = 1;

  return flag;
}
