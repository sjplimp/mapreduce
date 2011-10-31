// fanout/in test between 2 sets of nodes in MPI
// sending streams of integers between procs in all2all mode

// build as:
// g++4 -O -DMPICH_SKIP_MPICXX fanoutin.cpp -lmpich -lpthread -o fanoutin
// g++ -I ~/tools/zeromq-2.0.10/local/include ping.cpp -L ~/tools/zeromq-2.0.10/local/lib -lzmq -o ping

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"

#define NMAX 1000000

int main(int narg, char **args)
{
  MPI_Init(&narg,&args);
  int me,nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD,&me);
  MPI_Comm_size(MPI_COMM_WORLD,&nprocs);

  if (narg != 2) {
    printf("Syntax: fanoutin N\n");
    exit(1);
  }

  int nnum = atoi(args[1]);
  int nhalf = nprocs/2;

  if (nnum <= 0 || 2*nhalf != nprocs) {
    printf("ERROR: N > 0 and P = even required\n");
    exit(1);
  }

  // init RNG with time

  int iseed = static_cast<int> (MPI_Wtime());
  srand(iseed);

  // send/recv loop
  // lower half of procs are senders, upper half are receivers

  double time_start = MPI_Wtime();

  int count = 0;
    
  if (me < nhalf) {
    int j,num;
    for (int i = 0; i < nnum+nhalf; i++) {
      if (i < nnum) {
	num = static_cast<int> (((double) rand()) / RAND_MAX * NMAX);
	j = nhalf*num/NMAX;
      } else {
	num = -1;
	j = i - nnum;
      }
      MPI_Send(&num,1,MPI_INT,j+nhalf,0,MPI_COMM_WORLD);
    }

  } else {
    MPI_Status status;
    int num;
    int donecount = 0;

    while (donecount < nhalf) {
      MPI_Recv(&num,1,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&status);
      if (num < 0) donecount++;
      else count++;
    }
  }

  double time_stop = MPI_Wtime();

  // print stats

  if (me < nhalf)
    printf("Sent count = %d in %g secs\n",nnum,time_stop-time_start);
  else
    printf("Accumulated count = %d in %g secs\n",count,time_stop-time_start);

  MPI_Finalize();
}
