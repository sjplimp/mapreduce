
// Code simplifying use of local disks in different build environments.
// Karen Devine, March 2010

#ifndef _LOCALDISKS_HPP
#define _LOCALDISKS_HPP

/////////////////////////////////////////////////////////////////////////////
// MRMPI_FPATH can be defined in the build environment, allowing it to be
// specified easily on different architectures.  If it is not specified,
// the current working directory is used. 
// Applications can then specify MYLOCALDISK as a macro when setting 
// the local disk for a MapReduce object.

#ifdef MRMPI_FPATH
// This magic converts a MRMPI_FPATH specified with a -D compiler flag
// to a string that can be passed as an argument to MapReduce methods.
#define _QUOTEME(x) #x
#define QUOTEME(x) _QUOTEME(x)
#define MYLOCALDISK QUOTEME(MRMPI_FPATH)

#else

#define MYLOCALDISK "."

#endif

#ifndef MRMPI_MEMSIZE
#define MRMPI_MEMSIZE 64
#endif

/////////////////////////////////////////////////////////////////////////////
// Test the file system for writing; make sure nodes can write to disks.
// On odin, the local file system sometimes is bad on a node or two.
// We'd like to detect this problem before we do other computation.

void test_local_disks() 
{
  int me;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  char filename[29];
  sprintf(filename, "%s/test.%03d", MYLOCALDISK, me);
  FILE *fp = fopen(filename, "w");
  if (fp == NULL) {
    char name[252];
    int len;
    MPI_Get_processor_name(name, &len);
    name[len]='\0';
    printf("ERROR IN THE LOCAL DISK SYSTEM\n");
    fflush(stdout);
    printf("RANK %d NODE %s: CANNOT OPEN TEST FILE ON LOCAL DISK %s. (%d %s)\n",
           me, name, MYLOCALDISK, me, name);
    fflush(stdout);
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  fclose(fp);
  remove(filename);
}

/////////////////////////////////////////////////////////////////////////////
void greetings()
{
  // A sanity-check message from the processor.
  int me, np;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &np);

  char name[252];
  int len;
  MPI_Get_processor_name(name, &len);
  name[len]='\0';

  printf("GREETINGS FROM RANK %d of %d, NODE %s.\n",
         me, np, name);
}


#endif
