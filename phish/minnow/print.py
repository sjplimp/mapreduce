#!/usr/local/bin/python

import sys
import phish

def myprint(nvalues):
  for i in range(nvalues):
    type,value,len = phish.unpack()
    if type == phish.RAW:
      pass
    elif type < phish.INT_ARRAY:
      print value,
    else:
      for val in value: print val,
  print
  
args = phish.init(sys.argv)
phish.input(0,myprint,None,1)
phish.check()

phish.loop()
phish.exit()
