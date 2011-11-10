// emit edges from an RMAT matrix

#include "mpi.h"
#include "stdint.h"
#include "stdlib.h"
#include "string.h"
#include "stdio.h"
#include "phish.h"

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init(&narg,&args);
  phish_output(1);
  phish_check();

  if (narg != 8) phish_error("Rmat syntax: rmat N M a b c d fraction seed");

  uint64_t ngenerate = atol(args[0]); 
  int nlevels = atoi(args[1]); 
  double a = atof(args[2]); 
  double b = atof(args[3]); 
  double c = atof(args[4]); 
  double d = atof(args[5]); 
  double fraction = atof(args[6]); 
  int seed = atoi(args[7]);

  if (a + b + c + d != 1.0) phish_error("Rmat a,b,c,d must sum to 1");
  if (fraction >= 1.0) phish_error("Rmat fraction must be < 1");
  if (seed <= 0) phish_error("Rmat seed must be positive integer");

  // perturb seed for each proc in case running multiple SPs

  int me;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  srand48(seed+me);
  uint64_t order = 1L << nlevels;

  uint64_t i,j,delta;
  int ilevel;
  double a1,b1,c1,d1,total,rn;

  for (uint64_t m = 0; m < ngenerate; m++) {
    delta = order >> 1;
    a1 = a; b1 = b; c1 = c; d1 = d;
    i = j = 0;

    for (ilevel = 0; ilevel < nlevels; ilevel++) {
      rn = drand48();
      if (rn < a1) {
      } else if (rn < a1+b1) {
	j += delta;
      } else if (rn < a1+b1+c1) {
	i += delta;
      } else {
	i += delta;
	j += delta;
      }
      
      delta /= 2;
      if (fraction > 0.0) {
	a1 += a1*fraction * (drand48() - 0.5);
	b1 += b1*fraction * (drand48() - 0.5);
	c1 += c1*fraction * (drand48() - 0.5);
	d1 += d1*fraction * (drand48() - 0.5);
	total = a1+b1+c1+d1;
	a1 /= total;
	b1 /= total;
	c1 /= total;
	d1 /= total;
      }
    }

    phish_pack_uint64(i);
    phish_pack_uint64(j);
    phish_send(0);
  }

  phish_exit();
}
