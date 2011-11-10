// reverse strings read from stdin, one line at a time, to stdout

#include "stdlib.h"
#include "string.h"
#include "stdio.h"

#define MAXLINE 1024

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  char line[MAXLINE],rline[MAXLINE];
  char *ptr;

  while ((ptr = fgets(line,MAXLINE,stdin)) != NULL) {
    int n = strlen(line) - 1;
    for (int i = 0; i < n; i++) rline[n-i-1] = line[i];
    rline[n] = '\0';
    printf("%s\n",rline);
  }
}
