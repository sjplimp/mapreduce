// count words

#include "string.h"
#include "stdio.h"
#include "unistd.h"
#include "zmq.h"
#include "zhelpers.h"

int main () {
  void *context = zmq_init(1);

  void *receiver = zmq_socket(context,ZMQ_PULL);
  zmq_connect(receiver,"tcp://localhost:5555");

  int count = 0;

  while (1) {
    char *string = s_recv(receiver);
    if (strlen(string) == 0) break;
    count++;
    free(string);
  }
  
  printf("Word Count = %d\n",count);

  zmq_close(receiver);
  zmq_term(context);
}
