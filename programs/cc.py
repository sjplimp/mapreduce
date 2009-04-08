#!/usr/bin/python

# Connected Components

import sys

# map and reduce functions

def map1(itask,kv):
  eij = (vi,vj)
  kv.add(vi,eij)
  kv.add(vj,eij)

def reduce1(key,mvalue,kv):
  for edge in mvalue:
    kv.add(edge,key)
    
def reduce2(key,mvalue,kv):
  zi,zj = mvalue[0],mvalue[1]
  if zi == zj:
    kv.add(zi,(key,zi))
  else:
    kv.add(zi,(key,zi))
    kv.add(zj,(key,zi))
    
def reduce3(key,mvalue,kv):
  zmin = BIG
  for value in mvalue:
    zmin = MIN(zmin,value[1])
  for value in mvalue:
    kv.add(value[0][0],zmin)
    kv.add(value[0][1],zmin)

def reduce4(key,mvalue,kv):
  zmin = BIG
  for value in mvalue:
    zmin = MIN(zmin,value[1])
  for value in mvalue:
    kv.add(value[0],zmin)

# main program

if len(sys.argv) < 2:
  print "Syntax: cc.py file1 file2 ..."
  sys.exit()
files = sys.argv[1:]

mr = mapreduce()
mr.map(len(files),map1)
mr.collate()
mr.reduce(reduce1)

while 1:
  mr.collate()
  mr.reduce(reduce2)
  mr.collate()
  mr.reduce(reduce3)
  if doneflag: break
  mr.collate()
  mr.reduce(reduce4)
