
// Code simplifying use of out-of-core MR-MPI.
// Karen Devine, March 2010

#ifndef _BLOCKMACROS_HPP
#define _BLOCKMACROS_HPP

// Macros defining how to loop over blocks when multivalue is stored in
// more than one block.  These macros are used frequently, so we define them 
// here.
// Each reduce function should initially CHECK_FOR_BLOCKS to get the
// number of blocks for the multivalue. 
// Then code to be executed on each block should be between a BEGIN_BLOCK_LOOP
// and an END_BLOCK_LOOP.
// Note:  This mechanism is a little clunky.  Make sure you DO NOT have a 
// semicolon afer these macros.

#define CHECK_FOR_BLOCKS(multivalue, valuebytes, nvalues, totalnvalues)  \
  int bbb_nblocks = 1; \
  total_nvalues = nvalues; \
  MapReduce *bbb_mr = NULL; \
  if (!(multivalue)) { \
    bbb_mr = (MapReduce *) (valuebytes); \
    total_nvalues = bbb_mr->multivalue_blocks(bbb_nblocks); \
  } 

#define BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)  \
  for (int bbb_iblock = 0; bbb_iblock < bbb_nblocks; bbb_iblock++) { \
    if (bbb_mr)  \
      (nvalues) = bbb_mr->multivalue_block(bbb_iblock, \
                                           &(multivalue),&(valuebytes)); 

#define BREAK_BLOCK_LOOP break
#define END_BLOCK_LOOP } 

#endif
