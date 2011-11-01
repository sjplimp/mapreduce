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
  printf("PING %d %s %s %s %s\n",narg,args[0],args[1],args[2],args[5]);
  phish_init("ping",1,1,&narg,&args);
  phish_callback_datum(ping);

  if (narg != 2) phish_error("Ping syntax: ping N M");
  n = atoi(args[0]);
  int m = atoi(args[1]);

  buf = new char[m];
  for (int i = 0; i < m; i++) buf[i] = '\0';
  count = 0;

  double time_start = phish_timer();

  phish_pack_string(buf);
  phish_send();
  phish_loop();

  double time_stop = phish_timer();
  printf("Elapsed time for %d pingpong of %d bytes = %g secs\n",
	 n,m,time_stop-time_start);

  delete [] buf;
  phish_close();
}

/* ---------------------------------------------------------------------- */

void ping(int nvalues)
{
  char *buf;
  int len;

  if (nvalues != 1) phish_error("Ping processes one-value datums");
  int type = phish_unpack_next(&buf,&len);
  if (type != PHISH_STRING) phish_error("Ping processes string values");

  count++;
  if (count < n) {
    phish_pack_string(buf);
    phish_send();
  } else phish_send_done();
}
