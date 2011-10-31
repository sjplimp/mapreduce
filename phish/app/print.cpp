// print datums to screen or file

#include "stdlib.h"
#include "string.h"
#include "stdio.h"
#include "phish.h"

void print(int);
void close_file();

/* ---------------------------------------------------------------------- */

FILE *fp = NULL;

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init("print",1,0,&narg,&args);
  phish_callback_datum(print);
  phish_callback_done(close_file);

  int iarg = 0;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-f") == 0) {
      if (iarg+1 > narg) phish_error("Print syntax: print -f filename");
      fp = fopen(args[iarg+1],"w");
      iarg += 2;
    } else phish_error("Print syntax: print -f filename");
  }

  if (fp == NULL) fp = stdout;

  phish_loop();
  phish_close();
}

/* ---------------------------------------------------------------------- */

void print(int nvalues)
{
  char *value;
  int len;

  for (int i = 0; i < nvalues; i++) {
    int type = phish_unpack_next(&value,&len);
    if (type == PHISH_STRING) fprintf(fp,"%s ",value);
    else if (type == PHISH_INT) fprintf(fp,"%d ",*(int *) value);
    else if (type == PHISH_UINT64) fprintf(fp,"%lu ",*(uint64_t *) value);
    else if (type == PHISH_DOUBLE) fprintf(fp,"%g ",*(double *) value);
  }
  fprintf(fp,"\n");
}

/* ---------------------------------------------------------------------- */

void close_file()
{
  if (fp != stdout) fclose(fp);
}
