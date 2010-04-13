#include "stdlib.h"
#include "string.h"
#include "sys/stat.h"
#include "map_file_read.h"
#include "error.h"

#include "keyvalue.h"

using namespace APP_NS;
using MAPREDUCE_NS::KeyValue;

/* ---------------------------------------------------------------------- */

MapFileRead::MapFileRead(APP *app, char *idstr, int narg, char **arg) : 
  Map(app, idstr)
{
  if (narg) error->all("Illegal map file_read command");

  appmap_file_list = map;
  appptr = this;
}

/* ---------------------------------------------------------------------- */

void MapFileRead::map(int itask, char *file, KeyValue *kv, void *ptr)
{
  struct stat stbuf;
  int flag = stat(file,&stbuf);
  if (flag < 0) {
    MapFileRead *data = (MapFileRead *) ptr;
    data->error->one("Could not query file size");
  }
  int filesize = stbuf.st_size;

  FILE *fp = fopen(file,"r");
  char *text = new char[filesize+1];
  int nchar = fread(text,1,filesize,fp);
  text[nchar] = '\0';
  fclose(fp);

  char *whitespace = " \t\n\f\r\0";
  char *word = strtok(text,whitespace);
  while (word) {
    kv->add(word,strlen(word)+1,NULL,0);
    word = strtok(NULL,whitespace);
  }

  delete [] text;
}
