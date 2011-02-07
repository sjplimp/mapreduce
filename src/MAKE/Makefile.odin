# odin = Odin cluster in CSRI, g++, MPICH

SHELL = /bin/sh

# ---------------------------------------------------------------------
# compiler/archive settings
# specify flags and libraries needed for your compiler and MPI installation

CC =		mpic++ -m64
CCFLAGS =	-O2 -DMRMPI_FPATH=/localdisk1/scratch 
DEPFLAGS =	-M
ARCHIVE =	ar
ARFLAGS =	-rc
SIZE =		size

# ---------------------------------------------------------------------
# build rules and dependencies
# no need to edit this section

# Library target

lib:	$(OBJ)
	$(ARCHIVE) $(ARFLAGS) $(EXE) $(OBJ)

# Compilation rules

%.o:%.cpp
	$(CC) $(CCFLAGS) $(EXTRA_INC) -c $<

%.d:%.cpp
	$(CC) $(CCFLAGS) $(EXTRA_INC) $(DEPFLAGS) $< > $@

# Individual dependencies

DEPENDS = $(OBJ:.o=.d)
include $(DEPENDS)
