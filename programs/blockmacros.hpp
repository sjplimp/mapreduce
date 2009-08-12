
// Code simplifying use of out-of-core MR-MPI.

#ifndef _BLOCKMACROS_HPP
#define _BLOCKMACROS_HPP

#ifdef NEW_OUT_OF_CORE

// Macros defining how to loop over blocks when multivalue is stored in
// more than one block.  These macros are used frequently, so we define them 
// here.
// Each reduce function should initially CHECK_FOR_BLOCKS to get the
// number of blocks for the multivalue. 
// Then code to be executed on each block should be between a BEGIN_BLOCK_LOOP
// and an END_BLOCK_LOOP.
// Note:  This mechanism is a little clunky.  Make sure you DO NOT have a 
// semicolon afer these macros.

#define CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues)  \
  int bbb_nblocks = 1; \
  MapReduce *bbb_mr = NULL; \
  if (!(multivalue)) { \
    bbb_mr = (MapReduce *) (valuebytes); \
    bbb_nblocks = bbb_mr->multivalue_blocks(); \
  } 

#define BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)  \
  for (int bbb_iblock = 0; bbb_iblock < bbb_nblocks; bbb_iblock++) { \
    if (bbb_mr)  \
      (nvalues) = bbb_mr->multivalue_block(bbb_iblock, \
                                           &(multivalue),&(valuebytes)); 

#define BREAK_BLOCK_LOOP break
#define END_BLOCK_LOOP } 

#else  // !NEW_OUT_OF_CORE

// These macros are not needed with the in-core mapreduce library.
// We'll define them as no-ops (with curly braces so compilation is consistent),
// though, so we don't need as many #ifdefs in the code.

#define CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues) 
#define BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues) {

#define BREAK_BLOCK_LOOP 
#define END_BLOCK_LOOP }

#endif  // NEW_OUT_OF_CORE

/////////////////////////////////////////////////////////////////////////////
void test_local_disks(char *root) 
{
// Test the file system for writing; make sure nodes can write to disks.
  int me;
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  char filename[29];
  sprintf(filename, "%s/test.%03d", root, me);
  FILE *fp = fopen(filename, "w");
  if (fp == NULL) {
    char name[252];
    int len;
    MPI_Get_processor_name(name, &len);
    name[len]='\0';
    printf("RANK %d NODE %s:   CANNOT OPEN TEST FILE ON LOCAL DISK %s.\n",
           me, name, root);
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
