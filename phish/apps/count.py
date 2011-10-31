#!/usr/local/bin/python

def count(buf,nbytes):
  if hash.has_key(buf): hash[buf] = hash[buf] + 1
  else: hash[buf] = 1

def done(bytes,nbytes):
  pairs = hash.items()
  for k,v in pairs:
    phish.send("%d %s" % (v,k))
  phish.send_done()

# -------------------------
  
import sys,os,glob,copy
from phish import Phish

phish = Phish()

args = phish.init("count",1,1,sys.argv)

phish.callback(phish.DATUM,count)
phish.callback(phish.DONE,done)

if len(args) != 0:
  print "Count syntax: count"
  sys.exit()

hash = {}
  
phish.loop()
phish.close()
