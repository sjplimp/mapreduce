#ifndef RANMARS_H
#define RANMARS_H

class RanMars {
 public:
  RanMars(int);
  ~RanMars();
  double uniform();

 private:
  int seed,save;
  double second;
  double *u;
  int i97,j97;
  double c,cd,cm;
};

#endif
