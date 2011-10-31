// read file and emit words

#include "string.h"
#include "stdio.h"
#include "phish.h"

void read_file(int);
void done();

#define MAXLINE 1024

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init("file2words",1,1,&narg,&args);
  phish_callback_datum(read_file);
  phish_callback_done(done);

  if (narg != 0) phish_error("File2words syntax: file2words");

  phish_loop();
  phish_close();
}

/* ---------------------------------------------------------------------- */

void read_file(int nvalues)
{
  char *filename;
  int len;

  if (nvalues != 1) phish_error("File2words processes one-value datums");
  int type = phish_unpack_next(&filename,&len);
  if (type != PHISH_STRING) phish_error("File2words processes string values");

  int n;
  char line[MAXLINE];
  const char *whitespace = " \t\n\f\r\0";

  FILE *fp = fopen(filename,"r");
  while (fgets(line,MAXLINE,fp)) {
    char *word = strtok(line,whitespace);
    while (word) {
      phish_pack_string(word);
      phish_send_key(word,strlen(word));
      word = strtok(NULL,whitespace);
    }
  }
  fclose(fp);
}

/* ---------------------------------------------------------------------- */

void done()
{
  phish_send_done();
}
