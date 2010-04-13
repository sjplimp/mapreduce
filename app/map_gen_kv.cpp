#include "math.h"
#include "stdlib.h"
#include "string.h"
#include "map_gen_kv.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

#define INTMAX 0x7FFFFFFF

/* ---------------------------------------------------------------------- */

MapGenKV::MapGenKV(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg < 2) error->all("Illegal map gen_kv command");

  ntotal = atoi(arg[0]);
  int seed = atoi(arg[1]);

  // parse optional args

  ktype = 0;
  vtype = 1;
  klenlo = klenhi = vlenlo = vlenhi = 1;
  klo = 1;
  khi = INTMAX;
  vlo = 'a';
  vhi = 'z';

  int iarg = 2;
  while (iarg < narg) {
    if (strcmp(arg[iarg],"key") == 0) {
      if (iarg+6 > narg) error->all("Illegal map gen_kv command");
      if (strcmp(arg[iarg+1],"int") == 0) ktype = 0;
      else if (strcmp(arg[iarg+1],"str") == 0) ktype = 1;
      else error->all("Illegal map gen_kv command");
      klenlo = atoi(arg[iarg+2]);
      klenhi = atoi(arg[iarg+3]);
      if (ktype == 0) {
	klo = atoi(arg[iarg+4]);
	khi = atoi(arg[iarg+5]);
      } else { 
	if (strlen(arg[iarg+4]) != 1) error->all("Illegal map gen_kv command");
	kclo = arg[iarg+4][0];
	if (strlen(arg[iarg+5]) != 1) error->all("Illegal map gen_kv command");
	kchi = arg[iarg+5][0];
      }
      iarg += 6;
    } else if (strcmp(arg[iarg],"value") == 0) {
      if (iarg+6 > narg) error->all("Illegal map gen_kv command");
      if (strcmp(arg[iarg+1],"int") == 0) vtype = 0;
      else if (strcmp(arg[iarg+1],"str") == 0) vtype = 1;
      else error->all("Illegal map gen_kv command");
      vlenlo = atoi(arg[iarg+2]);
      vlenhi = atoi(arg[iarg+3]);
      if (vtype == 0) {
	vlo = atoi(arg[iarg+4]);
	vhi = atoi(arg[iarg+5]);
      } else { 
	if (strlen(arg[iarg+4]) != 1) error->all("Illegal map gen_kv command");
	vclo = arg[iarg+4][0];
	if (strlen(arg[iarg+5]) != 1) error->all("Illegal map gen_kv command");
	vchi = arg[iarg+5][0];
      }
      iarg += 6;
    } else error->all("Illegal map gen_kv command");
  }

  // error check

  if (klenlo <= 0 || klenlo > klenhi)
    error->all("Illegal map gen_kv command");
  if (ktype == 0 && (klo <= 0 || klo > khi))
    error->all("Illegal map gen_kv command");
  if (ktype == 1 && (kclo < 'a' || kchi > 'z' || kclo > kchi))
    error->all("Illegal map gen_kv command");

  if (vlenlo <= 0 || vlenlo > vlenhi)
    error->all("Illegal map gen_kv command");
  if (vtype == 0 && (vlo <= 0 || vlo > vhi))
    error->all("Illegal map gen_kv command");
  if (vtype == 1 && (vclo < 'a' || vchi > 'z' || vclo > vchi))
    error->all("Illegal map gen_kv command");

  klenrange = klenhi - klenlo + 1;
  vlenrange = vlenhi - vlenlo + 1;

  if (ktype == 0) krange = khi - klo + 1;
  else krange = kchi - kclo + 1;

  if (vtype == 0) vrange = vhi - vlo + 1;
  else vrange = vchi - vclo + 1;

  kint = vint = NULL;
  if (ktype == 0) kint = new int[klenhi];
  if (vtype == 0) vint = new int[vlenhi];

  kstr = vstr = NULL;
  if (ktype == 1) kstr = new char[klenhi+1];
  if (vtype == 1) vstr = new char[vlenhi+1];

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  nlocal = ntotal/nprocs;
  if (me < ntotal % nprocs) nlocal++;

  srand48(seed+me);

  appmap = map;
  appptr = (void *) this;
}

/* ---------------------------------------------------------------------- */

MapGenKV::~MapGenKV()
{
  delete [] kint;
  delete [] vint;
  delete [] kstr;
  delete [] vstr;
}

/* ---------------------------------------------------------------------- */

void MapGenKV::map(int itask, KeyValue *kv, void *ptr)
{
  MapGenKV *data = (MapGenKV *) ptr;
  int nlocal = data->nlocal;
  int klenlo = data->klenlo;
  int klenhi = data->klenhi;
  int vlenlo = data->vlenlo;
  int vlenhi = data->vlenhi;
  int klo = data->klo;
  int vlo = data->vlo;
  char kclo = data->kclo;
  char vclo = data->vclo;

  int klenrange = data->klenrange;
  int vlenrange = data->vlenrange;
  int krange = data->krange;
  int vrange = data->vrange;
  int *kint = data->kint;
  int *vint = data->vint;
  char *kstr = data->kstr;
  char *vstr = data->vstr;
  int klen,vlen;

  if (data->ktype == 0 && data->vtype == 0) {
    for (uint64_t i = 0; i < nlocal; i++) {
      klen = static_cast<int> (klenlo + klenrange*drand48());
      for (int j = 0; j < klen; j++)
	kint[j] = static_cast<int> (klo + krange*drand48());
      vlen = static_cast<int> (vlenlo + vlenrange*drand48());
      for (int j = 0; j < vlen; j++)
	vint[j] = static_cast<int> (vlo + vrange*drand48());
      kv->add((char *) kint,klen*sizeof(int),(char *) vint,vlen*sizeof(int));
    }
  } else if (data->ktype == 0 && data->vtype == 1) {
    for (uint64_t i = 0; i < nlocal; i++) {
      klen = static_cast<int> (klenlo + klenrange*drand48());
      for (int j = 0; j < klen; j++)
	kint[j] = static_cast<int> (klo + krange*drand48());
      vlen = static_cast<int> (vlenlo + vlenrange*drand48());
      for (int j = 0; j < vlen; j++)
	vstr[j] = static_cast<int> (vclo + vrange*drand48());
      vstr[vlen] = '\0';
      kv->add((char *) kint,klen*sizeof(int),vstr,vlen+1);
    }
  } else if (data->ktype == 1 && data->vtype == 0) {
    for (uint64_t i = 0; i < nlocal; i++) {
      klen = static_cast<int> (klenlo + klenrange*drand48());
      for (int j = 0; j < klen; j++)
	kstr[j] = static_cast<int> (kclo + krange*drand48());
      kstr[klen] = '\0';
      vlen = static_cast<int> (vlenlo + vlenrange*drand48());
      for (int j = 0; j < vlen; j++)
	vint[j] = static_cast<int> (vlo + vrange*drand48());
      kv->add(kstr,klen+1,(char *) vint,vlen*sizeof(int));
    }
  } else if (data->ktype == 1 && data->vtype == 1) {
    for (uint64_t i = 0; i < nlocal; i++) {
      klen = static_cast<int> (klenlo + klenrange*drand48());
      for (int j = 0; j < klen; j++)
	kstr[j] = static_cast<int> (kclo + krange*drand48());
      kstr[klen] = '\0';
      vlen = static_cast<int> (vlenlo + vlenrange*drand48());
      for (int j = 0; j < vlen; j++)
	vstr[j] = static_cast<int> (vclo + vrange*drand48());
      vstr[vlen] = '\0';
      kv->add(kstr,klen+1,vstr,vlen+1);
    }
  }
}
