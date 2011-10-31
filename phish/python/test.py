#!/usr/local/bin/python

from mpi import MPI
mpi = MPI()
mpi.Init(0)

world = mpi.COMM_WORLD

me = mpi.Comm_rank(world)
nprocs = mpi.Comm_size(world)

#print "ID",me,nprocs,mpi.COMM_WORLD,mpi.ANY_TAG,mpi.ANY_SOURCE,mpi.SUM

if me == 0:
  n = 100
  mpi.Bcast(n,0,mpi.COMM_WORLD)
else:
  n = 0
  mpi.Bcast(n,0,mpi.COMM_WORLD)

print "Bcast value:",me,n

mpi.Finalize()
