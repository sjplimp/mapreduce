#!/usr/local/bin/python

# Jon Cohen algorithm to enumerate triangles in a graph
# implemented by Steve Plimpton, 15 Apr 2009
# Syntax: try.py infile outfile
#   infile = matrix market file of edges
#   outfile = list of triangles, 3 vertices per line

import sys
import pypar
from mrmpi import mrmpi

# ----------------------------------------------------------------------
# maps and reduces

# read portion of input file
# all procs trim last line (between newline and NULL)
# 1st proc trims header line
# emit one KV per edge: key = (vi,vj), value = None

def fileread(itask,str,mr):
  lines = str.split("\n")
  lines.pop()
  if itask == 0: lines.pop(0)
  for line in lines:
    words = line.split()
    mr.add((int(words[0]),int(words[1])),None)

# invert edges so all have vi < vj
# drop edges where vi = vj

def invert_edges(key,mvalue,mr):
  if key[0] < key[1]: mr.add((key[0],key[1]),None)
  elif key[1] < key[0]: mr.add((key[1],key[0]),None)

# remove duplicate edges
# if I,J and J,I were in graph, now just one copy with I < J will be

def remove_duplicates(key,mvalue,mr):
  mr.add(key,None)

# convert edge key to vertex keys
# emit two KV per edge: (vi,vj) and (vj,vi)

def emit_vertices(key,mvalue,mr):
  mr.add(key[0],key[1])
  mr.add(key[1],key[0])

# assign degree of 1st vertex
# emit one KV per edge: ((vi,vj),(degree,0)) or ((vi,vj),(0,degree))
# where vi < vj and degree is assigned to correct vertex

def first_degree(key,mvalue,mr):
  degree = len(mvalue)
  for value in mvalue:
    if key < value: mr.add((key,value),(degree,0))
    else: mr.add((value,key),(0,degree))

# assign degree of 2nd vertex
# 2 values per edge, with degree of vi and vj
# emit one KV per edge: ((vi,vj),(deg(i),deg(j))) with vi < vj

def second_degree(key,mvalue,mr):
  if mvalue[0][0] != 0: mr.add(key,(mvalue[0][0],mvalue[1][1]))
  else: mr.add(key,(mvalue[1][0],mvalue[0][1]))

# low-degree vertex emits (vi,vj)
# break tie with low-index vertex

def low_degree(key,mvalue,mr):
  di = mvalue[0][0]
  dj = mvalue[0][1]
  if di < dj: mr.add(key[0],key[1])
  elif dj < di: mr.add(key[1],key[0])
  elif key[0] < key[1]: mr.add(key[0],key[1])
  else: mr.add(key[1],key[0])

# emit Nsq angles associated with each central vertex vi
# emit KV as ((vj,vk),vi) where vj < vk

def nsq_angles(key,mvalue,mr):
  n = len(mvalue)
  for i in range(n-1):
    for j in range(i+1,n):
      if mvalue[i] < mvalue[j]: mr.add((mvalue[i],mvalue[j]),key)
      else: mr.add((mvalue[j],mvalue[i]),key)

# if None exists in mvalue, emit other values as triangles
# emit KV as ((vi,vj,vk),None)

def emit_triangles(key,mvalue,mr):
  if None in mvalue:
    for value in mvalue:
      if value: mr.add((value,key[0],key[1]),None)

# print triangles to local file

def output_triangle(key,mvalue,mr):
  print >>fp,key[0],key[1],key[2]

# ----------------------------------------------------------------------
# main program

# setup

nprocs = pypar.size()
me = pypar.rank()

if len(sys.argv) != 3:
  if me == 0: print "Syntax: tri.py infile outfile"
  sys.exit()
infile = sys.argv[1]
outfile = sys.argv[2]

mr = mrmpi()
#mr.verbosity(1)

pypar.barrier()
tstart = pypar.time()

# read input file
# results in ((vi,vj),None)

nedges = mr.map_file_char(nprocs,[infile],"\n",80,fileread)
if me == 0: print nedges,"edges in input file"

# eliminate duplicate edges = both I,J and J,I exist
# results in ((vi,vj),None) with all vi < vj

mr.clone()
mr.reduce(invert_edges)
mr.collate()
nedges = mr.reduce(remove_duplicates)
if me == 0: print nedges,"edges after duplicates removed"

# make copy of graph for use in triangle finding

mrcopy = mr.copy()

# augment edges with degree of each vertex
# results in ((vi,vj),(deg(vi),deg(vj)) with all vi < vj

mr.clone()
mr.reduce(emit_vertices)
mr.collate()
mr.reduce(first_degree)
mr.collate()
mr.reduce(second_degree)
if me == 0: print nedges,"edges augmented by vertex degrees"

# find triangles in degree-augmented graph and write to output file
# once create angles, add in edges from original graph
# this enables finding completed triangles in emit_triangles()
# results in ((vi,vj,vk),None)

mr.clone()
mr.reduce(low_degree)
mr.collate()
mr.reduce(nsq_angles)
mr.add_kv(mrcopy)
mr.collate()
ntri = mr.reduce(emit_triangles)
if me == 0: print ntri,"triangles"

fp = open(outfile + "." + str(me),"w")
if not fp:
  print "ERROR: Could not open output file"
  sys.exit()
mr.clone()
mr.reduce(output_triangle)
fp.close()

# timing data

pypar.barrier()
tstop = pypar.time()
if me == 0:
  print "%g secs to find triangles on %d procs" % (tstop-tstart,nprocs)

# clean up

mrcopy.destroy()
mr.destroy()
pypar.finalize()
