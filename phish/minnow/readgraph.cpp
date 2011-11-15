// read file and emit CC commands

#include "string.h"
#include "stdlib.h"
#include "stdio.h"
#include "phish.h"

#define MAXLINE 1024

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init(&narg,&args);
  phish_output(0);
  phish_check();

  if (narg != 1) phish_error("Readgraph syntax: readgraph infile");

  FILE *fp = fopen(args[0],"r");
  if (fp == NULL) phish_error("Readgraph could not open infile");

  char line[MAXLINE];
  char *ptr,*word;
  int ivec[32];
  int nvec;

  while (fgets(line,MAXLINE,fp)) {
    char *word = strtok(line," \t\n");
    phish_pack_byte(word[0]);
    nvec = 0;
    word = strtok(NULL," \t\n");
    while (word) {
      ivec[nvec++] = atoi(word);
      word = strtok(NULL," \t\n");
    }
    phish_pack_int_array(ivec,nvec);
    phish_send(0);
  }
  
  fclose(fp);

  phish_exit();
}
