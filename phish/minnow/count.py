#!/usr/local/bin/python

def count(nvalues):
  type = phish.unpack(buf,len)
  if hash.has_key(buf): hash[buf] = hash[buf] + 1
  else: hash[buf] = 1

def sort():
  pairs = hash.items()
  for k,v in pairs:
    phish.pack_int(v)
    phish.pack_string(k)
    phish.send(0)

# -------------------------
  
import sys,os,glob,copy
from phish import Phish

phish = Phish()

args = phish.init(sys.argv)
phish.input(0,count,sort,1)
phish.output(0)
phish.check()

if len(args) != 0:
  print "Count syntax: count"
  sys.exit()

hash = {}
  
phish.loop()
phish.exit()
