#!/usr/local/bin/python

import sys,os,glob,copy
from phish import Phish

def count(nvalues):
  if nvalues != 1: phish.error("Count processes one-value datums")
  type,str,tmp = phish.unpack()
  #if type != phish.PHISH_STRING: phish.error("File2words processes string values")
  if hash.has_key(str): hash[str] = hash[str] + 1
  else: hash[str] = 1

def sort():
  pairs = hash.items()
  for key,value in pairs:
    phish.pack_int(value)
    phish.pack_string(key)
    phish.send(0)

phish = Phish()

args = phish.init(sys.argv)
phish.input(0,count,sort,1)
phish.output(0)
phish.check()

if len(args) != 0: phish.error("Count syntax: count")

hash = {}
  
phish.loop()
phish.exit()
