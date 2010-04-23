# Odin/MPI Makefile for MapReduce programs

OUTOFCORE = -DMRMPI_FPATH=/localdisk1/scratch -DNEW_OUT_OF_CORE 

CC =		mpic++ -m64 
CCFLAGS =	-g -I../src  -D__STDC_LIMIT_MACROS $(OUTOFCORE)
LINK =		mpic++ -m64
LINKFLAGS =	-g $(OUTOFCORE)
USRLIB =	-L../src -lmrmpi
SYSLIB =	
LIB =		../src/libmrmpi.a
DEPFLAGS =      -M

include Makefile.common

