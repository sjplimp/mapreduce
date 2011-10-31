// pingpong test between 2 nodes in MPI
// Syntax: mpirun -np 2 pingpong N Nb
//         N = number of back-and-forth messages
//         Nb = # of bytes per message >= 0

// build as:
// g++4 -O -DMPICH_SKIP_MPICXX pingpong.cpp -lmpich -lpthread -o pingpong
// /opt/openmpi-gnu-1.3.2/bin/mpiCC pingpong.cpp -o pingpong

#include "mpi.h"
#include "stdlib.h"
#include "stdio.h"

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);
  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  if (narg != 3) {
    if (me == 0) printf("Syntax: pingpong N Nb\n");
    exit(1);
  }

  int nloop = atoi(args[1]);
  int nbytes = atoi(args[2]);

  if (nloop <= 0 || nbytes < 0) {
    if (me == 0) printf("ERROR: N > 0 and Nb >= 0 required\n");
    exit(1);
  }
  if (nprocs != 2) {
    if (me == 0) printf("ERROR: must run on 2 processors\n");
    exit(1);
  }

  char *buf = new char[nbytes];
  for (int i = 0; i < nbytes; i++) buf[i] = '\0';

  MPI_Request request;
  MPI_Status status;

  MPI_Barrier(MPI_COMM_WORLD);
  double time_start = MPI_Wtime();

  for (int i = 0; i < nloop; i++) {
    if (me == 0) {
      MPI_Send(buf,nbytes,MPI_BYTE,1,0,MPI_COMM_WORLD);
      MPI_Irecv(buf,nbytes,MPI_BYTE,1,1,MPI_COMM_WORLD,&request);
      MPI_Wait(&request,&status);
    } else {
      MPI_Irecv(buf,nbytes,MPI_BYTE,0,0,MPI_COMM_WORLD,&request);
      MPI_Wait(&request,&status);
      MPI_Send(buf,nbytes,MPI_BYTE,0,1,MPI_COMM_WORLD);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  double time_stop = MPI_Wtime();

  if (me == 0) printf("Elapsed time for %d pingpong of %d bytes = %g secs\n",
		      nloop,nbytes,time_stop-time_start);

  MPI_Finalize();
}
