// 2nd half of fanout/fanin test
// sending streams of integers between procs in all2all mode
// should launch fanin nodes first

// build as:
// g++4 fanin.cpp -lzmq -o fanin
// g++ -I ~/tools/zeromq-2.0.10/local/include ping.cpp -L ~/tools/zeromq-2.0.10/local/lib -lzmq -o ping

#include "stdio.h"
#include "stdlib.h"
#include <sys/time.h>
#include "zmq.hpp"

#define OFFSET 5555

double MPI_Wtime();

int main(int narg, char **args)
{
  if (narg != 4) {
    printf("Syntax: fanin N P I\n");
    exit(1);
  }

  int nnum = atoi(args[1]);
  int nfan = atoi(args[2]);
  int ifan = atoi(args[3]);

  if (nnum <= 0 || nfan <= 0) {
    printf("ERROR: N > 0 and P > 0 required\n");
    exit(1);
  }

  void *context = zmq_init(1);

  // one socket to subscribe to all fanout procs

  void *socket = zmq_socket(context,ZMQ_SUB);
  char tcp[32];
  for (int i = 0; i < nfan; i++) {
    sprintf(tcp,"tcp://*:%d",i*nfan+ifan+OFFSET);
    zmq_connect(socket,tcp);
  }
  zmq_setsockopt(socket,ZMQ_SUBSCRIBE,NULL,0);

  // loop until receive enough done messages

  double time_start = MPI_Wtime();

  int num;
  int donecount = 0;
  int count = 0;
  zmq_msg_t message;

  while (donecount < nfan) {
    zmq_msg_init(&message);
    zmq_recv(socket,&message,0);
    num = *(int *) zmq_msg_data(&message);
    zmq_msg_close(&message);

    if (num < 0) donecount++;
    else count++;
  }

  double time_stop = MPI_Wtime();

  // print stats

  printf("Accumulated count = %d in %g secs\n",count,time_stop-time_start);

  zmq_close(socket);
  zmq_term(context);
}

double MPI_Wtime()
{
  double time;
  struct timeval tv;

  gettimeofday(&tv,NULL);
  time = 1.0 * tv.tv_sec + 1.0e-6 * tv.tv_usec;
  return time;
}
