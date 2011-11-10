// read tab-separated file and emit specified fields from each line

#include "mpi.h"
#include "string.h"
#include "stdlib.h"
#include "stdio.h"
#include "ctype.h"
#include "phish.h"

#define MAXLINE 1024
#define MAXWORD 64

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init(&narg,&args);
  phish_check();

  phish_loop();
  phish_exit();
}
