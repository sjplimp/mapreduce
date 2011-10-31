// read one or more files and emit words

#include "string.h"
#include "stdio.h"
#include "unistd.h"
#include "zmq.h"
#include "zhelpers.h"

#define MAXLINE 1024

int main (int narg, char **arg) {
  void *context = zmq_init(1);

  void *sender = zmq_socket(context,ZMQ_PUSH);
  zmq_bind(sender,"tcp://*:5555");

  char line[MAXLINE];
  char *whitespace = " \t\n\f\r\0";

  for (int ifile = 1; ifile < narg; ifile++) {
    FILE *fp = fopen(arg[ifile],"r");
    while (fgets(line,MAXLINE,fp)) {
      char *word = strtok(line,whitespace);
      while (word) {
	s_send(sender,word);
	word = strtok(NULL,whitespace);
      }
    }
    fclose(fp);
  }

  s_send(sender,"");
	
  zmq_close(sender);
  zmq_term(context);
}
