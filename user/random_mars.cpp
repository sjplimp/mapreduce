// Marsaglia random number generator

#include "mpi.h"
#include "math.h"
#include "stdio.h"
#include "stdlib.h"
#include "random_mars.h"

/* ---------------------------------------------------------------------- */

RanMars::RanMars(int seed)
{
  int ij,kl,i,j,k,l,ii,jj,m;
  double s,t;

  if (seed <= 0 || seed > 900000000) {
    int me;
    MPI_Comm_rank(MPI_COMM_WORLD,&me);
    if (me == 0) printf("Invalid seed for Marsaglia random # generator\n");
    MPI_Finalize();
    exit(1);
  }

  save = 0;
  u = new double[97+1];

  ij = (seed-1)/30082;
  kl = (seed-1) - 30082*ij;
  i = (ij/177) % 177 + 2;
  j = ij %177 + 2;
  k = (kl/169) % 178 + 1;
  l = kl % 169;
  for (ii = 1; ii <= 97; ii++) {
    s = 0.0;
    t = 0.5;
    for (jj = 1; jj <= 24; jj++) {
      m = ((i*j) % 179)*k % 179;
      i = j;
      j = k;
      k = m;
      l = (53*l+1) % 169;
      if ((l*m) % 64 >= 32) s = s + t;
      t = 0.5*t;
    }
    u[ii] = s;
  }
  c = 362436.0 / 16777216.0;
  cd = 7654321.0 / 16777216.0;
  cm = 16777213.0 / 16777216.0;
  i97 = 97;
  j97 = 33;
  uniform();
}

/* ---------------------------------------------------------------------- */

RanMars::~RanMars()
{
  delete [] u;
}

/* ----------------------------------------------------------------------
   uniform RN 
------------------------------------------------------------------------- */

double RanMars::uniform()
{
  double uni = u[i97] - u[j97];
  if (uni < 0.0) uni += 1.0;
  u[i97] = uni;
  i97--;
  if (i97 == 0) i97 = 97;
  j97--;
  if (j97 == 0) j97 = 97;
  c -= cd;
  if (c < 0.0) c += cm;
  uni -= c;
  if (uni < 0.0) uni += 1.0;
  return uni;
}
