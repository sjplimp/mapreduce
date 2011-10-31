// read datum and emit it with slowdown delay

#include "string.h"
#include "stdio.h"
#include "stdlib.h"
#include "unistd.h"
#include "phish.h"

void send(int);
void done();

/* ---------------------------------------------------------------------- */

double delta;
double time_previous;

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init("slowdown",1,1,&narg,&args);
  phish_callback_datum(send);
  phish_callback_done(done);

  if (narg != 1) phish_error("Slowdown syntax: slowdown delta");
  delta = atof(args[0]);

  time_previous = phish_timer();

  phish_loop();
  phish_close();
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

  phish_unpack_raw(&buf,&len);
  phish_pack_raw(nvalues,buf,len);
  phish_send();

  time_previous = phish_timer();
}

/* ---------------------------------------------------------------------- */

void done()
{
  phish_send_done();
}
