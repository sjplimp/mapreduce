# Odin/MPI Makefile for MapReduce programs

OUTOFCORE = -DMRMPI_FPATH=/localdisk1/scratch -DNEW_OUT_OF_CORE 

CC =		mpic++ -m64 
CCFLAGS =	-O2 -I../src  -D__STDC_LIMIT_MACROS $(OUTOFCORE)
LINK =		mpic++ -m64
LINKFLAGS =	-O2 $(OUTOFCORE)
USRLIB =	-L../src -lmrmpi
SYSLIB =	
LIB =		../src/libmrmpi.a
DEPFLAGS =      -M

include Makefile.common

