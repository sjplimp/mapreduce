// reflect messages to a sender

#include "stdlib.h"
#include "phish.h"

void pong(int);
void done();

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init("pong",1,1,&narg,&args);
  phish_callback_datum(pong);
  phish_callback_done(done);

  if (narg != 0) phish_error("Pong syntax: pong");

  phish_loop();
  phish_close();
}

/* ---------------------------------------------------------------------- */

void pong(int nvalues)
{
  char *buf;
  int len;

  if (nvalues != 1) phish_error("Pong processes one-value datums");
  int type = phish_unpack_next(&buf,&len);
  if (type != PHISH_STRING) phish_error("Pong processes string values");

  phish_pack_string(buf);
  phish_send();
}

/* ---------------------------------------------------------------------- */

void done()
{
  phish_send_done();
}
