// reflect messages to a receiver

#include "stdlib.h"
#include "stdio.h"
#include "phish.h"

int n,count;
char *buf;

void ping(int);

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init(&narg,&args);
  phish_input(0,ping,NULL,1);
  phish_output(1);
  phish_check();

  if (narg != 2) phish_error("Ping syntax: ping N M");
  n = atoi(args[0]);
  int m = atoi(args[1]);

  buf = new char[m];
  for (int i = 0; i < m; i++) buf[i] = '\0';
  count = 0;

  double time_start = phish_timer();

  phish_pack_string(buf);
  phish_send(0);
  phish_loop();

  double time_stop = phish_timer();
  printf("Elapsed time for %d pingpong of %d bytes = %g secs\n",
	 n,m,time_stop-time_start);

  delete [] buf;
  phish_exit();
}

/* ---------------------------------------------------------------------- */

void ping(int nvalues)
{
  char *buf;
  int len;

  if (nvalues != 1) phish_error("Ping processes one-value datums");
  int type = phish_unpack(&buf,&len);
  if (type != PHISH_STRING) phish_error("Ping processes string values");

  count++;
  if (count < n) {
    phish_pack_string(buf);
    phish_send(0);
  } else phish_close(0);
}
