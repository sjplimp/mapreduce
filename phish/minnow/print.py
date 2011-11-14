#!/usr/local/bin/python

def myprint(nvalues):
  for i in range(nvalues):
    type,value,len = phish.unpack()
    print value,
  print

# -------------------------

import sys
from phish import *
  
phish = Phish()

args = phish.init(sys.argv)
phish.input(0,myprint,None,1)
phish.check()

phish.loop()
phish.exit()
