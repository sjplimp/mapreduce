#!/usr/local/bin/python

import sys,os,glob,copy
from phish import Phish

phish = Phish()

args = phish.init("filegen",0,1,sys.argv)

if len(args) == 0:
  print "Filegen syntax: filegen.py file1 file2 ..."
  sys.exit()

files = []

for file in args:
  if os.path.isfile(file): files.append(file)
  else: files += glob.glob("%s/*" % file)

for file in files:
  phish.pack_string(file)
  phish.send()

phish.send_done()
phish.close()
