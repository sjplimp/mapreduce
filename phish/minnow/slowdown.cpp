// read datum and emit it with slowdown delay

#include "string.h"
#include "stdio.h"
#include "stdlib.h"
#include "unistd.h"
#include "phish.h"

void send(int);

/* ---------------------------------------------------------------------- */

double delta;
double time_previous;

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init(&narg,&args);
  phish_input(0,send,NULL,1);
  phish_output(0);
  phish_check();

  if (narg != 1) phish_error("Slowdown syntax: slowdown delta");
  delta = atof(args[0]);

  time_previous = phish_timer();

  phish_loop();
  phish_exit();
}

/* ---------------------------------------------------------------------- */

void send(int nvalues)
{
  double elapsed = phish_timer() - time_previous;
  if (elapsed < delta) {
    useconds_t usec = (useconds_t) (1000000.0 * (delta-elapsed));
    usleep(usec);
  }

  char *buf;
  int len;

  phish_datum(&buf,&len);
  phish_pack_datum(buf,len);
  phish_send(0);

  time_previous = phish_timer();
}
