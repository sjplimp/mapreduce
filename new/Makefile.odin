# MPI-based Makefile using mpic++ and mpicc.

# Define a directory to use as local disk.
LOCALDISK   = /localdisk1/scratch

CC =		mpic++ 
CCFLAGS =	-O2
DEPFLAGS =	-M
ARCHIVE =	ar
ARFLAGS =	-rc

include Makefile.common

