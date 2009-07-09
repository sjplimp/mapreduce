# Mac/MPI Makefile for MapReduce programs

CC =		mpic++ -m64
CCFLAGS =	-g -I../src -DODIN
#CCFLAGS += -DNEW_OUT_OF_CORE
LINK =		mpic++ -m64 -DODIN
LINKFLAGS =	-g 
#LINKFLAGS += -DNEW_OUT_OF_CORE
USRLIB =	-L../src -lmrmpi
SYSLIB =	
LIB =		../src/libmrmpi.a


include Makefile.common

