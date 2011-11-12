// wrap a child process which creates datums by writing to stdout
// read datums from child, one by one, via a pipe

#include "stdlib.h"
#include "string.h"
#include "stdio.h"
#include "phish.h"

#define MAXLINE 1024

void launch(int);

/* ---------------------------------------------------------------------- */

char program[MAXLINE] = "";

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init(&narg,&args);

  if (narg < 1) phish_error("Wrapsource syntax: wrapsource -f program");

  // check for -f switch to run child multiple times on incoming datums

  int fileflag = 0;
  if (strcmp(args[0],"-f") == 0) {
    phish_input(0,launch,NULL,1);
    fileflag = 1;
  }

  phish_output(0);
  phish_check();

  // combine remaining args into one string to launch with popen()
  // would be better if there was exactly one arg
  // but mpiexec strips quotes from quoted args

  for (int i = fileflag; i < narg; i++) {
    strcat(program,args[i]);
    if (i < narg-1) strcat(program," ");
  }

  // in -f mode, loop on incoming datums to run child program multiple times
  // else just run child program once

  if (fileflag) phish_loop();
  else launch(0);

  phish_exit();
}

/* ---------------------------------------------------------------------- */

void launch(int nvalues)
{
  char cmd[MAXLINE];

  if (nvalues == 0) strcpy(cmd,program);
  else if (nvalues == 1) {
    char *buf;
    int len;
    int type = phish_unpack(&buf,&len);
    if (type != PHISH_STRING) phish_error("Wrapsource processes string values");
    sprintf(cmd,program,buf);
  } else phish_error("Wrapsource processes one-value datums");

  // launch child process

  FILE *fp = popen(cmd,"r");

  // read all output lines that child produces and send them downstream

  char line[MAXLINE];
  char *eof;
  int n;

  while (1) {
    eof = fgets(line,MAXLINE,fp);
    if (eof == NULL) break;
    n = strlen(line);
    line[n-1] = '\0';
    phish_pack_string(line);
    phish_send(0);
  }

  pclose(fp);
}
