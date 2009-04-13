#!/usr/local/bin/python

/* ----------------------------------------------------------------------
   MR-MPI = MapReduce-MPI library
   http://www.cs.sandia.gov/~sjplimp/mapreduce.html
   Steve Plimpton, sjplimp@sandia.gov, Sandia National Laboratories

   Copyright (2009) Sandia Corporation.  Under the terms of Contract
   DE-AC04-94AL85000 with Sandia Corporation, the U.S. Government retains
   certain rights in this software.  This software is distributed under 
   the Berkeley Software Distribution (BSD) License.

   See the README file in the top-level MapReduce directory.
------------------------------------------------------------------------- */

import sys
import pypar
from mrmpi import mrmpi

def map(itask,mr,dummy):
  text = open(files[itask]).read()
  words = text.split()
  for word in words: mr.add(word,None)

def reduce(key,mvalue,mr):
  mr.add(key,len(mvalue))

def compare(key1,key2):
  if key1 < key2: return -1
  elif key1 > key2: return 1
  else: return 0

def output(key,mvalue,mr):
  count[0] += 1
  if count[0] > count[1]: return
  print key,mvalue

# main program

nprocs = pypar.size()
me = pypar.rank()

if len(sys.argv) < 2:
  print "Syntax: wc.py file1 file2 ..."
  sys.exit()
files = sys.argv[1:]

pypar.barrier()
time1 = pypar.time()

m = mrmpi()

nkey = m.map(len(files),map)
nunique = m.collate()
m.reduce(reduce)

m.sort_keys(compare)
m.collate()
count = [0,10]
m.reduce(output)

pypar.barrier()
time2 = pypar.time()

if me == 0:
  print "Nwords,Nunique",nkey,nunique
  print "%g seconds on %d procs" % (time2-time1,nprocs)
  
pypar.finalize()
