// wrap a child process which consumes datums by reading from stdin
// write datums to child, one by one, via a pipe

#include "stdlib.h"
#include "string.h"
#include "stdio.h"
#include "phish.h"

#define MAXLINE 1024

void sink(int);
void close();

/* ---------------------------------------------------------------------- */

FILE *fp = NULL;

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init("wrapsink",1,0,&narg,&args);
  phish_callback_datum(sink);
  phish_callback_done(close);

  // combine all args into one string to launch with popen()
  // would be better if there was exactly one arg
  // but mpiexec strips quotes from quoted args

  if (narg < 1) phish_error("Wrapsink syntax: wrapsink program");
  char program[1024];
  for (int i = 0; i < narg; i++) {
    strcat(program,args[i]);
    if (i < narg-1) strcat(program," ");
  }

  fp = popen(program,"w");

  phish_loop();
  phish_close();
}

/* ---------------------------------------------------------------------- */

void sink(int nvalues)
{
  char *str;
  int len;

  if (nvalues != 1) phish_error("Warpsink processes one-value datums");
  int type = phish_unpack_next(&str,&len);
  if (type != PHISH_STRING) phish_error("Wrapsink processes string values");

  fprintf(fp,"%s\n",str);
}

/* ---------------------------------------------------------------------- */

void close()
{
  pclose(fp);
}
