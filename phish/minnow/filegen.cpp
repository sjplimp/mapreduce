// emit filenames

#include "mpi.h"
#include "string.h"
#include "stdio.h"
#include "stdlib.h"
#include "ctype.h"
#include "sys/stat.h"
#include "dirent.h"
#include "phish.h"

#define MAXLINE 1024
#define FILECHUNK 128

void findfiles(char *, int, int, int &, int &, char **&);
void addfile(char *, int, int &, int &, char **&);

/* ---------------------------------------------------------------------- */

int nfile = 0;
int maxfile = 0;
char **files = NULL;

/* ---------------------------------------------------------------------- */

int main(int narg, char **args)
{
  phish_init(&narg,&args);
  phish_output(0);
  phish_check();

  if (narg < 1) phish_error("Filegen syntax: filegen file1 file2 ...");

  int recurse = 1;
  int readflag = 0;

  for (int i = 0; i < narg; i++)
    findfiles(args[i],recurse,readflag,nfile,maxfile,files);

  for (int i = 0; i < nfile; i++) {
    phish_pack_string(files[i]);
    phish_send(0);
  }

  for (int i = 0; i < nfile; i++) delete [] files[i];
  free(files);

  phish_exit();
}

/* ----------------------------------------------------------------------
   use str to find files to add to list of filenames
   if str is a file, add it to list
   if str is a directory, add all files in directory to list
   if recurse = 1, call findfiles() on any directory found within directory
   return updated list of files
------------------------------------------------------------------------- */

void findfiles(char *str, int recurse, int readflag,
	       int &nfile, int &maxfile, char **&files)
{
  int err,n;
  struct stat buf;
  char newstr[MAXLINE];

  err = stat(str,&buf);
  if (err) {
    char msg[256];
    sprintf(msg,"Could not query status of file %s in filegen",str);
    phish_error(msg);
  }
  else if (S_ISREG(buf.st_mode)) addfile(str,readflag,nfile,maxfile,files);
  else if (S_ISDIR(buf.st_mode)) {
    struct dirent *ep;
    DIR *dp = opendir(str);
    if (dp == NULL) {
      char msg[256];
      sprintf(msg,"Cannot open directory %s to search for files in filegen",
	      str);
      phish_error(msg);
    }
    while (ep = readdir(dp)) {
      if (ep->d_name[0] == '.') continue;
      sprintf(newstr,"%s/%s",str,ep->d_name);
      err = stat(newstr,&buf);
      if (S_ISREG(buf.st_mode)) addfile(newstr,readflag,nfile,maxfile,files);
      else if (S_ISDIR(buf.st_mode) && recurse)
	findfiles(newstr,recurse,readflag,nfile,maxfile,files);
    }
    closedir(dp);
  } else {
    char msg[256];
    sprintf(msg,"Invalid filename %s in filegen",str);
    phish_error(msg);
  }
}

/* ----------------------------------------------------------------------
   add a str to list of filenames
   if readflag = 0, just add str as filename
   if readflag = 1, open the file, read filenames out of it and add each to list
   return updated list of files
------------------------------------------------------------------------- */

void addfile(char *str, int readflag, int &nfile, int &maxfile, char **&files)
{
  if (!readflag) {
    if (nfile == maxfile) {
      maxfile += FILECHUNK;
      files = (char **) realloc(files,maxfile*sizeof(char *));
    }
    int n = strlen(str) + 1;
    files[nfile] = new char[n];
    strcpy(files[nfile],str);
    nfile++;
    return;
  }

  FILE *fp = fopen(str,"r");
  if (fp == NULL) {
    char msg[256];
    sprintf(msg,"Could not open file %s of filenames in filegen",str);
    phish_error(msg);
  }

  char line[MAXLINE];

  while (fgets(line,MAXLINE,fp)) {
    char *ptr = line;
    while (isspace(*ptr)) ptr++;
    if (strlen(ptr) == 0) {
      char msg[256];
      sprintf(msg,"Blank line in file %s of filenames in filegen",str);
      phish_error(msg);
    }
    char *ptr2 = ptr + strlen(ptr) - 1;
    while (isspace(*ptr2)) ptr2--;
    ptr2++;
    *ptr2 = '\0';

    if (nfile == maxfile) {
      maxfile += FILECHUNK;
      files = (char **) realloc(files,maxfile*sizeof(char *));
    }
    
    int n = strlen(ptr) + 1;
    files[nfile] = new char[n];
    strcpy(files[nfile],ptr);
    nfile++;
  }

  fclose(fp);
}
