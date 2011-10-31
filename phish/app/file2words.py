#!/usr/local/bin/python

def read_file(nvalues):
  if nvalues != 1: phish.error("File2words processes one-value datums")
  type,filename,len = phish.unpack_next()
      # how to do this
      # what kind of type is it
  if type != phish.STRING: phish.error("File2words processes string values")
  
  lines = open(filename,"r").readlines()
  for line in lines:
    words = line.split()
    for word in words:
      phish.pack_string(word)
      phish.send_key(word)

def done():
  phish.send_done()

# -------------------------
  
import sys,os,glob,copy
from phish import Phish

phish = Phish()

args = phish.init("file2words",1,1,sys.argv)

phish.callback_datum(read_file)
phish.callback_done(done)

if len(args) != 0:
  print "File2words syntax: file2words"
  sys.exit()

phish.loop()
phish.close()

