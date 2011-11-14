// echo strings read from stdin, one line at a time, to stdout

#include "stdlib.h"
#include "string.h"
#include "stdio.h"

#define MAXLINE 1024

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  char line[MAXLINE];
  char *ptr;

  while (ptr = fgets(line,MAXLINE,stdin)) {
    fprintf(stderr,"ECHO %d %s",strlen(line),line);
    printf("%s",line);
  }
}
