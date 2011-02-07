/* ----------------------------------------------------------------------
   OINK - scripting wrapper on MapReduce-MPI library
   http://www.sandia.gov/~sjplimp/mapreduce.html, Sandia National Laboratories
   Steve Plimpton, sjplimp@sandia.gov

   See the README file in the top-level MR-MPI directory.
------------------------------------------------------------------------- */

// data structure for RMAT parameters

struct RMAT_struct {
  int nlevels,order;
  int nnonzero;
  int ngenerate;
  double a,b,c,d,fraction;
};
