#!/usr/local/bin/python

import sys
import phish

def myprint(nvalues):
  for i in range(nvalues):
    type,value,len = phish.unpack()
    if type == phish.RAW:
      pass
    elif type < phish.INT_ARRAY:
      print >>fp,value,
    else:
      for val in value: print >>fp,val,
  print >>fp

args = phish.init(sys.argv)
phish.input(0,myprint,None,1)
phish.check()

fp = sys.stdout

iarg = 0
while iarg < len(args):
  if args[iarg] == "-f":
    if iarg+1 > len(args): phish.error("Print syntax: print -f filename");
    fp = open(args[iarg+1],"w")
    iarg += 2
  else: phish.error("Print syntax: print -f filename")

phish.loop()
phish.exit()
