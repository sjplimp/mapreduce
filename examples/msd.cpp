/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

// MapReduce mean-squared displacement computation
// Syntax: msd switch args switch args ...
// switches:
//   -s 0/1 = scaled =  0: coords are not scaled, 1: scaled, default = 1
//   -x 3 4 5 = xyz = columns for x,y,z coords, default = 3,4,5
//   -i 7 8 9 = image = columns for image flags, default = no image flags
//   -t 1 2 = type = only include atoms of this type, default = 0 = all types
//   -e 0/1 = every = 0: no intervals, 1: compute all time intervals, def = 1
//   -f file1 file2 ... = files = list of files to read in
// writes MSD data to tmp.out

// needs to be mean of squares for everyflag = 1 -> values are sqs not disps
// implement choice of ID and type col, inclusion of only some types
// need to allow for boxsize to change when apply PBC if no image flags
// implement -c chunksize to process atoms in chunks for huge problem?

#include "mpi.h"
#include "math.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "mapreduce.h"
#include "keyvalue.h"

using namespace MAPREDUCE_NS;

#define MAXLINE 256

void fileread(int, KeyValue *, void *);
void snapread(int, char *, int, KeyValue *, void *);
int tcompare(char *, int, char *, int);
void displacement(char *, int, char *, int, int *, KeyValue *, void *);
void remap(double &, double, double, double);
void meandisp(char *, int, char *, int, int *, KeyValue *, void *);
int icompare(char *, int, char *, int);
void output(char *, int, char *, int, int *, KeyValue *, void *);
void error(int, char *);
void errorone(char *);

struct Box {
  double xlo,xhi;
  double ylo,yhi;
  double zlo,zhi;
};

struct Atom {
  int time;
  double x,y,z;
};

struct MSD {
  int scaled;
  int idcol,tcol;
  int xcol,ycol,zcol;
  int imageflag;
  int ixcol,iycol,izcol;
  int typeflag;
  int *types;
  int everyflag;

  Box box;
  int nfiles;
  char **files;
};

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);

  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  // parse command-line args

  MSD msd;
  msd.scaled = 1;
  msd.idcol = 1;
  msd.tcol = 2;
  msd.xcol = 3;
  msd.ycol = 4;
  msd.zcol = 5;
  msd.imageflag = 0;
  msd.typeflag = 0;
  msd.everyflag = 1;
  msd.nfiles = 0;
  msd.files = NULL;

  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-s") == 0) {
      if (iarg+2 > narg) error(me,"Bad arguments");
      msd.scaled = atoi(args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-x") == 0) {
      if (iarg+4 > narg) error(me,"Bad arguments");
      msd.xcol = atoi(args[iarg+1]);
      msd.ycol = atoi(args[iarg+2]);
      msd.zcol = atoi(args[iarg+3]);
      iarg += 4;
    } else if (strcmp(args[iarg],"-i") == 0) {
      if (iarg+4 > narg) error(me,"Bad arguments");
      msd.imageflag = 1;
      msd.ixcol = atoi(args[iarg+1]);
      msd.iycol = atoi(args[iarg+2]);
      msd.izcol = atoi(args[iarg+3]);
      iarg += 4;
    } else if (strcmp(args[iarg],"-e") == 0) {
      if (iarg+2 > narg) error(me,"Bad arguments");
      msd.everyflag = atoi(args[iarg+1]);
      iarg += 2;
    } else if (strcmp(args[iarg],"-f") == 0) {
      iarg++;
      while (iarg < narg) {
	if (args[iarg][0] == '-') break;
	msd.files = (char **) realloc(msd.files,(msd.nfiles+1)*sizeof(char *));
	msd.files[msd.nfiles] = args[iarg];
	msd.nfiles++;
	iarg++;
      }
    } else error(me,"Bad arguments");
  }

  // perform MapReduce

  MPI_Barrier(MPI_COMM_WORLD);
  double tstart = MPI_Wtime();

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  mr->verbosity = 2;

  //mr->map(msd.nfiles,&fileread,&msd);
  mr->map(4,msd.nfiles,msd.files,"ITEM: TIMESTEP",150000,&snapread,&msd);

  mr->collate(NULL);
  mr->sort_multivalues(&tcompare);
  mr->reduce(&displacement,&msd);
  mr->collate(NULL);
  mr->reduce(&meandisp,NULL);
  mr->gather(1);
  mr->sort_keys(&icompare);
  mr->clone();

  FILE *fp = NULL;
  if (me == 0) {
    fp = fopen("tmp.out","w");
    fprintf(fp,"# Mean-squared displacement: time x y z total\n");
    fprintf(fp,"0 0.0 0.0 0.0 0.0\n");
  }
  mr->reduce(&output,fp);
  if (me == 0) fclose(fp);

  delete mr;

  MPI_Barrier(MPI_COMM_WORLD);
  double tstop = MPI_Wtime();

  // statistics

  if (me == 0) {
    printf("Time to compute MSD for %d files on %d procs = %g (secs)\n",
	   msd.nfiles,nprocs,tstop-tstart);
  }

  free(msd.files);

  MPI_Finalize();
}

/* ----------------------------------------------------------------------
   fileread map() function
   for each atom in snapshot, emit key = ID, value = (time,x,y,z)
------------------------------------------------------------------------- */

void fileread(int itask, KeyValue *kv, void *ptr)
{
  char line[MAXLINE];
  char *words[20];

  MSD *msd = (MSD *) ptr;
  FILE *fp = fopen(msd->files[itask],"r");

  int ntimestep,natoms;

  char *eof = fgets(line,MAXLINE,fp);
  eof = fgets(line,MAXLINE,fp);
  sscanf(line,"%d",&ntimestep);
  eof = fgets(line,MAXLINE,fp);
  eof = fgets(line,MAXLINE,fp);
  sscanf(line,"%d",&natoms);
  eof = fgets(line,MAXLINE,fp);
  eof = fgets(line,MAXLINE,fp);
  sscanf(line,"%lg %lg",&msd->box.xlo,&msd->box.xhi);
  eof = fgets(line,MAXLINE,fp);
  sscanf(line,"%lg %lg",&msd->box.ylo,&msd->box.yhi);
  eof = fgets(line,MAXLINE,fp);
  sscanf(line,"%lg %lg",&msd->box.zlo,&msd->box.zhi);
  eof = fgets(line,MAXLINE,fp);

  int scaled = msd->scaled;
  int idcol = msd->idcol - 1;
  int tcol = msd->tcol - 1;
  int xcol = msd->xcol - 1;
  int ycol = msd->ycol - 1;
  int zcol = msd->zcol - 1;

  int imageflag = msd->imageflag;
  int ixcol,iycol,izcol;
  if (imageflag) {
    ixcol = msd->ixcol;
    iycol = msd->iycol;
    izcol = msd->izcol;
  }

  double xlo = msd->box.xlo;
  double xhi = msd->box.xhi;
  double ylo = msd->box.ylo;
  double yhi = msd->box.yhi;
  double zlo = msd->box.zlo;
  double zhi = msd->box.zhi;
  double xprd = xhi - xlo;
  double yprd = yhi - ylo;
  double zprd = zhi - zlo;

  int *id = new int[natoms];
  Atom *atoms = new Atom[natoms];

  for (int i = 0; i < natoms; i++) {
    eof = fgets(line,MAXLINE,fp);
    words[0] = strtok(line," \n");
    int j = 1;
    while (words[j] = strtok(NULL," \n")) j++;

    id[i] = atoi(words[idcol]);
    atoms[i].time = ntimestep;
    if (scaled) {
      atoms[i].x = xlo + xprd*atof(words[xcol]);
      atoms[i].y = ylo + yprd*atof(words[ycol]);
      atoms[i].z = zlo + zprd*atof(words[zcol]);
    } else {
      atoms[i].x = atof(words[xcol]);
      atoms[i].y = atof(words[ycol]);
      atoms[i].z = atof(words[zcol]);
    }
    if (imageflag) {
      atoms[i].x += xprd*atoi(words[ixcol]);
      atoms[i].y += yprd*atoi(words[iycol]);
      atoms[i].z += zprd*atoi(words[izcol]);
    }
  }

  fclose(fp);

  kv->add(natoms,(char *) id,sizeof(int),(char *) atoms,sizeof(Atom));

  delete [] id;
  delete [] atoms;
}

/* ----------------------------------------------------------------------
   snapread map() function
   passed text of one or more snapshots from a file
   for each atom in each snapshot, emit key = ID, value = (time,x,y,z)
------------------------------------------------------------------------- */

void snapread(int itask, char *str, int size, KeyValue *kv, void *ptr)
{
  char *line;
  char *words[20];

  MSD *msd = (MSD *) ptr;

  char *next = strchr(str,'\n');
  while (next) {
    *next = '\0';
    int ntimestep,natoms;
    line = next + 1; next = strchr(line,'\n'); *next = '\0';
    sscanf(line,"%d",&ntimestep);
    line = next + 1; next = strchr(line,'\n'); *next = '\0';
    line = next + 1; next = strchr(line,'\n'); *next = '\0';
    sscanf(line,"%d",&natoms);
    line = next + 1; next = strchr(line,'\n'); *next = '\0';
    line = next + 1; next = strchr(line,'\n'); *next = '\0';
    sscanf(line,"%lg %lg",&msd->box.xlo,&msd->box.xhi);
    line = next + 1; next = strchr(line,'\n'); *next = '\0';
    sscanf(line,"%lg %lg",&msd->box.ylo,&msd->box.yhi);
    line = next + 1; next = strchr(line,'\n'); *next = '\0';
    sscanf(line,"%lg %lg",&msd->box.zlo,&msd->box.zhi);
    line = next + 1; next = strchr(line,'\n'); *next = '\0';

    int scaled = msd->scaled;
    int idcol = msd->idcol - 1;
    int tcol = msd->tcol - 1;
    int xcol = msd->xcol - 1;
    int ycol = msd->ycol - 1;
    int zcol = msd->zcol - 1;

    int imageflag = msd->imageflag;
    int ixcol,iycol,izcol;
    if (imageflag) {
      ixcol = msd->ixcol;
      iycol = msd->iycol;
      izcol = msd->izcol;
    }

    double xlo = msd->box.xlo;
    double xhi = msd->box.xhi;
    double ylo = msd->box.ylo;
    double yhi = msd->box.yhi;
    double zlo = msd->box.zlo;
    double zhi = msd->box.zhi;
    double xprd = xhi - xlo;
    double yprd = yhi - ylo;
    double zprd = zhi - zlo;

    int *id = new int[natoms];
    Atom *atoms = new Atom[natoms];

    for (int i = 0; i < natoms; i++) {
      line = next + 1; next = strchr(line,'\n'); *next = '\0';
      words[0] = strtok(line," \n");
      int j = 1;
      while (words[j] = strtok(NULL," \n")) j++;

      id[i] = atoi(words[idcol]);
      atoms[i].time = ntimestep;
      if (scaled) {
	atoms[i].x = xlo + xprd*atof(words[xcol]);
	atoms[i].y = ylo + yprd*atof(words[ycol]);
	atoms[i].z = zlo + zprd*atof(words[zcol]);
      } else {
	atoms[i].x = atof(words[xcol]);
	atoms[i].y = atof(words[ycol]);
	atoms[i].z = atof(words[zcol]);
      }
      if (imageflag) {
	atoms[i].x += xprd*atoi(words[ixcol]);
	atoms[i].y += yprd*atoi(words[iycol]);
	atoms[i].z += zprd*atoi(words[izcol]);
      }
    }

    kv->add(natoms,(char *) id,sizeof(int),(char *) atoms,sizeof(Atom));

    delete [] id;
    delete [] atoms;

    line = next + 1; next = strchr(line,'\n');
  }
}

/* ----------------------------------------------------------------------
   tcompare compare() function
   order values by timestamp
------------------------------------------------------------------------- */

int tcompare(char *p1, int len1, char *p2, int len2)
{
  int i1 = *(int *) p1;
  int i2 = *(int *) p2;
  if (i1 < i2) return -1;
  else if (i1 > i2) return 1;
  else return 0;
}

/* ----------------------------------------------------------------------
   displacement reduce() function
   for each interval in time course, emit key = dt, value = (dx,dy,dz)
   use image flags or box size to determine box crossings
------------------------------------------------------------------------- */

void displacement(char *key, int keybytes, char *multivalue,
		  int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  int i,j,m,n;
  int *time = new int[nvalues];
  int *dt = new int[nvalues-1];
  double *xyz = new double[3*nvalues];
  double *dxyz = new double[3*(nvalues-1)];

  MSD *msd = (MSD *) ptr;
  Atom *atom = (Atom *) multivalue;

  // extract trajectory from values

  m = 0;
  for (i = 0; i < nvalues; i++) {
    time[i] = atom[i].time;
    xyz[m++] = atom[i].x;
    xyz[m++] = atom[i].y;
    xyz[m++] = atom[i].z;
  }

  // PBC unwrap the trajectory if image flags were not used in map() function

  if (msd->imageflag == 0) {
    double xprd = msd->box.xhi - msd->box.xlo;
    double yprd = msd->box.yhi - msd->box.ylo;
    double zprd = msd->box.zhi - msd->box.zlo;
    double xhalf = 0.5*xprd;
    double yhalf = 0.5*yprd;
    double zhalf = 0.5*zprd;

    for (i = 1; i < nvalues; i++) {
      m = 3*i;
      n = m - 3;
      if (fabs(xyz[m]-xyz[n]) > xhalf) remap(xyz[m],xyz[n],xprd,xhalf);
      if (fabs(xyz[m+1]-xyz[n+1]) > yhalf) remap(xyz[m+1],xyz[n+1],yprd,yhalf);
      if (fabs(xyz[m+2]-xyz[n+2]) > zhalf) remap(xyz[m+2],xyz[n+2],zprd,zhalf);
    }
  }

  // setup time intervals
  // if everyflag, check that all intervals are identical

  for (i = 0; i < nvalues-1; i++)
    dt[i] = time[i+1] - time[0];
  if (msd->everyflag && nvalues > 2) {
    for (i = 1; i < nvalues-1; i++)
      if (dt[i] != (i+1)*dt[0]) errorone("Snapshots must be equally spaced");
  }

  // if everyflag, compute displacement for every interval
  // else just displacement from initial position

  if (msd->everyflag) {
    m = 0;
    for (i = 0; i < nvalues-1; i++) {
      dxyz[m++] = 0.0;
      dxyz[m++] = 0.0;
      dxyz[m++] = 0.0;
    }

    for (int delta = 1; delta < nvalues; delta++) {
      n = delta-1;
      for (i = delta; i < nvalues; i++) {
	j = i - delta;
	dxyz[3*n+0] += xyz[3*i+0] - xyz[3*j+0];
	dxyz[3*n+1] += xyz[3*i+1] - xyz[3*j+1];
	dxyz[3*n+2] += xyz[3*i+2] - xyz[3*j+2];
      }
    }

    m = 0;
    for (i = 1; i < nvalues; i++) {
      dxyz[m++] /= nvalues-i;
      dxyz[m++] /= nvalues-i;
      dxyz[m++] /= nvalues-i;
    }

  } else {
    m = 0;
    for (i = 1; i < nvalues; i++) {
      dxyz[m++] = xyz[3*i+0] - xyz[0];
      dxyz[m++] = xyz[3*i+1] - xyz[1];
      dxyz[m++] = xyz[3*i+2] - xyz[2];
    }
  }

  kv->add(nvalues-1,(char *) dt,sizeof(int),(char *) xyz,3*sizeof(double));

  delete [] time;
  delete [] dt;
  delete [] xyz;
  delete [] dxyz;
}

/* ----------------------------------------------------------------------
   remap coord to be closest image to origin
------------------------------------------------------------------------- */

void remap(double &coord, double origin, double prd, double half)
{
  if (coord < origin) {
    while (coord < origin) coord += prd;
    if (coord-origin > half) coord -= prd;
  } else {
    while (coord > origin) coord -= prd;
    if (origin-coord > half) coord += prd;
  }
}

/* ----------------------------------------------------------------------
   meandisp reduce() function = mean-squared displacement
   for each interval, emit key = dt, value = mean of (dx^2,dy^2,dz^2)
------------------------------------------------------------------------- */

void meandisp(char *key, int keybytes, char *multivalue,
	      int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  double xsqsum = 0.0;
  double ysqsum = 0.0;
  double zsqsum = 0.0;

  int offset = 0;
  for (int i = 0; i < nvalues; i++) {
    double *disp = (double *) &multivalue[offset];
    xsqsum += disp[0]*disp[0];
    ysqsum += disp[1]*disp[1];
    zsqsum += disp[2]*disp[2];
    offset += valuebytes[i];
  }

  double xyz[3];
  xyz[0] = xsqsum/nvalues;
  xyz[1] = ysqsum/nvalues;
  xyz[2] = zsqsum/nvalues;

  kv->add(key,keybytes,(char *) xyz,3*sizeof(double));
}

/* ----------------------------------------------------------------------
   isort compare() function
   order keys by time interval
------------------------------------------------------------------------- */

int icompare(char *p1, int len1, char *p2, int len2)
{
  int i1 = *(int *) p1;
  int i2 = *(int *) p2;
  if (i1 < i2) return -1;
  else if (i1 > i2) return 1;
  else return 0;
}

/* ----------------------------------------------------------------------
   output reduce() function
   print dt, msd to file
------------------------------------------------------------------------- */

void output(char *key, int keybytes, char *multivalue,
	    int nvalues, int *valuebytes, KeyValue *kv, void *ptr) 
{
  FILE *fp = (FILE *) ptr;
  int dt = *(int *) key;
  double *xyz = (double *) multivalue;
  double x = xyz[0];
  double y = xyz[1];
  double z = xyz[2];
  fprintf(fp,"%d %g %g %g %g\n",dt,x,y,z,x*x+y*y+z*z);
}

/* ---------------------------------------------------------------------- */

void error(int me, char *str)
{
  if (me == 0) printf("ERROR: %s\n",str);
  MPI_Abort(MPI_COMM_WORLD,1);
}

/* ---------------------------------------------------------------------- */

void errorone(char *str)
{
  printf("ERROR: %s\n",str);
  MPI_Abort(MPI_COMM_WORLD,1);
}
