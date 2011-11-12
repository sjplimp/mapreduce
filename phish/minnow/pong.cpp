// reflect messages to a sender

#include "stdio.h"
#include "stdlib.h"
#include "phish.h"

void pong(int);

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init(&narg,&args);
  phish_input(0,pong,NULL,1);
  phish_output(0);
  phish_check();

  if (narg != 0) phish_error("Pong syntax: pong");

  phish_loop();
  phish_exit();
}

/* ---------------------------------------------------------------------- */

void pong(int nvalues)
{
  char *buf;
  int len;

  if (nvalues != 1) phish_error("Pong processes one-value datums");
  int type = phish_unpack(&buf,&len);
  if (type != PHISH_STRING) phish_error("Pong processes string values");

  phish_pack_string(buf);
  phish_send(0);
}
