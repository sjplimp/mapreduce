#!/usr/local/bin/python

import sys

if len(sys.argv) == 1:
  print "Syntax: wordfreq.py file1 file2 ..."
  sys.exit()
  
dict = {}
for file in sys.argv[1:]:
  text = open(file,'r').read()
  words = text.split()
  for word in words:
    if word not in dict: dict[word] = 1
    else: dict[word] += 1
unique = dict.keys()
for word in unique:
  print dict[word],word
