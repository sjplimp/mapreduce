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
  int macro_nblocks = 1; \
  totalnvalues = nvalues; \
  MapReduce *macro_mr = NULL; \
  if (!(multivalue)) { \
    macro_mr = (MapReduce *) (valuebytes); \
    totalnvalues = macro_mr->multivalue_blocks(macro_nblocks); \
  } 

#define BEGIN_BLOCK_LOOP(multivalue, valuebytes, nvalues)  \
  for (int macro_iblock = 0; macro_iblock < macro_nblocks; macro_iblock++) { \
    if (macro_mr)  \
      (nvalues) = macro_mr->multivalue_block(macro_iblock, \
                                             &(multivalue),&(valuebytes)); 

#define BREAK_BLOCK_LOOP break
#define END_BLOCK_LOOP } 

#endif
