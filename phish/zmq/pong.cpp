// pong half of pingpong test between 2 procs in ZMQ
// should launch pong first in ZMQ_PAIR mode so waits on 1st recv

// build as:
// g++4 pong.cpp -lzmq -o pong
// g++ -I ~/tools/zeromq-2.0.10/local/include ping.cpp -L ~/tools/zeromq-2.0.10/local/lib -lzmq -o ping

#include "stdio.h"
#include "stdlib.h"
#include <sys/time.h>
#include "zmq.hpp"

double MPI_Wtime();

int main(int narg, char **args)
{
  if (narg != 3) {
    printf("Syntax: pong N Nb\n");
    exit(1);
  }

  int nloop = atoi(args[1]);
  int nbytes = atoi(args[2]);

  if (nloop <= 0 || nbytes < 0) {
    printf("ERROR: N > 0 and Nb >= 0 required\n");
    exit(1);
  }

  void *context = zmq_init(1);

  // pong is 1/2 of symmetric pair
  void *partner = zmq_socket(context,ZMQ_PAIR);
  // pong is server in request/reply
  //void *partner = zmq_socket(context,ZMQ_REP);

  zmq_bind(partner,"tcp://192.168.2.108:5555");
  //zmq_bind(partner,"tcp://*:5555");

  char *buf = new char[nbytes];
  for (int i = 0; i < nbytes; i++) buf[i] = '\0';

  zmq_msg_t message;

  double time_start = MPI_Wtime();

  for (int i = 0; i < nloop; i++) {
    zmq_msg_init(&message);
    zmq_recv(partner,&message,0);
    int size = zmq_msg_size(&message);
    if (size != nbytes) {
      printf("ERROR: Nbytes not received\n");
      exit(1);
    }
    memcpy(buf,zmq_msg_data(&message),nbytes);
    zmq_msg_close(&message);

    zmq_msg_init_size(&message,nbytes);
    memcpy(zmq_msg_data(&message),buf,nbytes);
    int rc = zmq_send(partner,&message,0);
    assert(!rc);
    zmq_msg_close(&message);
  }

  double time_stop = MPI_Wtime();

  printf("Elapsed time for %d pong of %d bytes = %g secs\n",
	 nloop,nbytes,time_stop-time_start);

  zmq_close(partner);
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
