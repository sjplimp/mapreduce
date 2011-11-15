#!/usr/local/bin/python

import sys,os,glob,copy
import phish

args = phish.init(sys.argv)
phish.output(0)
phish.check()

if len(args) != 1: phish.error("Readgraph syntax: readgraph.py infile")

lines = open(args[0],"r").readlines()

for line in lines:
  words = line.split()
  phish.pack_byte(words[0][0])
  ivec = [int(word) for word in words[1:]]
  phish.pack_int_array(ivec)
  phish.send(0)

phish.exit()
