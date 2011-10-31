// 1st half of fanout/fanin test
// sending streams of integers between procs in all2all mode
// should launch fanin nodes first

// build as:
// g++4 fanout.cpp -lzmq -o fanout
// g++ -I ~/tools/zeromq-2.0.10/local/include ping.cpp -L ~/tools/zeromq-2.0.10/local/lib -lzmq -o ping

#include "stdio.h"
#include "stdlib.h"
#include "unistd.h"
#include <sys/time.h>
#include "zmq.hpp"

#define NMAX 1000000
#define OFFSET 5555

double MPI_Wtime();

int main(int narg, char **args)
{
  if (narg != 4) {
    printf("Syntax: fanout N P I\n");
    exit(1);
  }

  int nnum = atoi(args[1]);
  int nfan = atoi(args[2]);
  int ifan = atoi(args[3]);

  if (nnum <= 0 || nfan <= 0) {
    printf("ERROR: N > 0 and P > 0 required\n");
    exit(1);
  }

  // init RNG with time

  int iseed = static_cast<int> (MPI_Wtime());
  srand(iseed);

  void *context = zmq_init(1);

  // one socket to publish to each fanin proc

  void **sockets = new void*[nfan];
  char tcp[32];
  for (int i = 0; i < nfan; i++) {
    sockets[i] = zmq_socket(context,ZMQ_PUB);
    sprintf(tcp,"tcp://*:%d",ifan*nfan+i+OFFSET);
    zmq_bind(sockets[i],tcp);
  }

  // loop until send all messages

  double time_start = MPI_Wtime();

  int j,num;
  zmq_msg_t message;

  for (int i = 0; i < nnum+nfan; i++) {
    if (i < nnum) {
      num = static_cast<int> (((double) rand()) / RAND_MAX * NMAX);
      j = nfan*num/NMAX;
    } else {
      num = -1;
      j = i - nnum;
    }

    if (num < 0) printf("IFAN %d sending done to socket %d\n",ifan,j);

    zmq_msg_init_size(&message,sizeof(int));
    memcpy(zmq_msg_data(&message),&num,sizeof(int));
    zmq_send(sockets[j],&message,0);
    zmq_msg_close(&message);
  }

  double time_stop = MPI_Wtime();

  // print stats

  printf("Sent count = %d in %g secs\n",nnum,time_stop-time_start);

  sleep(1);

  for (int i = 0; i < nfan; i++) zmq_close(sockets[i]);
  delete [] sockets;
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
