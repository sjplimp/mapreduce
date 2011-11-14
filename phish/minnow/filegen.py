#!/usr/local/bin/python

import sys,os,glob,copy
from phish import Phish

phish = Phish()

args = phish.init(sys.argv)
phish.output(0)
phish.check()

if len(args) == 0: phish.error("Filegen syntax: filegen.py file1 file2 ...")

files = []

for file in args:
  if os.path.isfile(file): files.append(file)
  else: files += glob.glob("%s/*" % file)

for file in files:
  phish.pack_string(file)
  phish.send(0)

phish.exit()
