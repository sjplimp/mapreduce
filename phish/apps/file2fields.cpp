// read tab-separated file and emit specified fields from each line

#include "mpi.h"
#include "string.h"
#include "stdlib.h"
#include "stdio.h"
#include "ctype.h"
#include "phish.h"

#define MAXLINE 1024
#define MAXWORD 64

void read_file(int);
void done();
char *host2domain(char *);

/* ---------------------------------------------------------------------- */

int nfields;
int *fields;

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init("file2fields",1,1,&narg,&args);
  phish_callback_datum(read_file);
  phish_callback_done(done);

  if (narg < 1) phish_error("File2fields syntax: file2words n1 n2 ...");
  nfields = narg;
  fields = new int[nfields];
  for (int iarg = 0; iarg < narg; iarg++) {
    fields[iarg] = atoi(args[iarg]);
    if (fields[iarg] <= 0) phish_error("Invalid field #");
  }

  phish_loop();

  delete [] fields;

  phish_close();
}

/* ---------------------------------------------------------------------- */

void read_file(int nvalues)
{
  char *filename;
  int len;

  if (nvalues != 1) phish_error("File2fields processes one-value datums");
  int type = phish_unpack_next(&filename,&len);
  if (type != PHISH_STRING) phish_error("File2fields processes string values");

  int i,m;
  char line[MAXLINE];
  char *words[MAXWORD];

  char *whitespace = "\t\n";

  FILE *fp = fopen(filename,"r");
  while (fgets(line,MAXLINE,fp)) {
    m = 0;
    words[m++] = strtok(line,whitespace);
    while (words[m-1]) words[m++] = strtok(NULL,whitespace);

    // app-specific transformation for domain->host

    words[fields[0]-1] = host2domain(words[fields[0]-1]);

    // output, word by word

    for (i = 0; i < nfields; i++)
      phish_pack_string(words[fields[i]-1]);

    // hash on first field

    phish_send_key(words[fields[0]-1],strlen(words[fields[0]-1]));
  }

  fclose(fp);
}

/* ---------------------------------------------------------------------- */

void done()
{
  phish_send_done();
}

/* ---------------------------------------------------------------------- */

char *host2domain(char *host)
{
  // port number = all digit field after trailing ':'
  // remove it

  char *ptr = strrchr(host,':');
  if (ptr) {
    char *ptr1;
    for (ptr1 = ptr+1; *ptr1; ptr1++)
      if (!isdigit(*ptr1)) break;
    if (!(*ptr1)) *ptr = '\0';
  }

  // if length < 6, return
  // if last character is a number, assume host is IP address, return
  
  if (strlen(host) < 6) return host;
  if (isdigit(host[strlen(host)-1])) return host;
  
  // domain = last 2 fields unless last field < 3 chars, then last 3 fields

  int len = strlen(host);
  int periodcnt = 0;
  int target_period = 2;
  int short_first_period = 0;

  for (int i = 0; i < len; i++) {
    int roffset = len - i - 1;
    if (host[roffset] == '.') {
      periodcnt++;
      if (periodcnt == 1) {
	switch(i) {
	case 0:
	  periodcnt--;
	case 1:
	case 2:
	  short_first_period = 1;
	  break;
	}
      }
      if (periodcnt == 2) {
	switch(i) {
	case 1:
	case 2:
	case 3:
	case 4:
	case 5:
	  target_period = 3;
	  break;
	case 6:
	  if (short_first_period) target_period = 3;
	}
      }
      if (target_period == periodcnt) return host+roffset+1;
    }
  }
  return host;
}
