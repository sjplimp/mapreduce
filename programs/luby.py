#!/usr/local/bin/python

# Luby's algorithm for finding a maximal independent set
# Syntax: luby.py infile outfile
#   infile = matrix market file of edges
#   outfile = list of vertices in maximal independent set

import sys
import pypar
from mrmpi import mrmpi

LOSER = 0
WINNER = 1
UNKNOWN = 2

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

def invert_edges(itask,key,value,mr):
  if key[0] < key[1]: mr.add((key[0],key[1]),None)
  elif key[1] < key[0]: mr.add((key[1],key[0]),None)

# remove duplicate edges
# if I,J and J,I were in graph, now just one copy with I < J will be

def remove_duplicates(key,mvalue,mr):
  mr.add(key,None)

# add pair of random #s to each edge
# just use RN = vertex ID for now
# this needs to be done correctly with vertex-consistent floating-point RN

def add_random(itask,key,value,mr):
  mr.add(key,(key[0],key[1]))

# play Luby game for each edge
# if any of multivalues is LOSER, just discard edge, marked on previous iter
# vertex with largest R is winner
# vertex with smallest R is loser
# tiebreak by vertex ID

def edge_game(key,mvalue,mr):
  if LOSER in mvalue: return
  ri = mvalue[0][0]
  rj = mvalue[0][1]
  if ri < rj:
    mr.add(key[0],(WINNER,ri,key[1],rj))
    mr.add(key[1],(LOSER,rj,key[0],ri))
  elif rj < ri:
    mr.add(key[1],(WINNER,rj,key[0],ri))
    mr.add(key[0],(LOSER,ri,key[1],rj))
  elif key[0] < [1]:
    mr.add(key[0],(WINNER,ri,key[1],rj))
    mr.add(key[1],(LOSER,rj,key[0],ri))
  else:
    mr.add(key[1],(WINNER,rj,key[0],ri))
    mr.add(key[0],(LOSER,ri,key[1],rj))

# mark winning vertices
# if I won all games, I am WINNER, my edge vertices are LOSERs
# emit all edge vertices as LOSER or UNKNOWN, emit self as WINNER (if so)

def mark_winners(key,mvalue,mr):
  flag = 1
  for value in mvalue:
    if value[0] != WINNER:
      flag = 0
      break
  if flag:
    mr.add(key,WINNER)
    for value in mvalue:
      mr.add(value[2],(LOSER,value[3],key,value[1]))
  else:
    for value in mvalue:
      mr.add(value[2],(UNKNOWN,value[3],key,value[1]))

# mark losing vertices and edges connected to losers
# if any mvalue for vertex is WINNER, add to vset and return
# if any mvalue for vertex is LOSER, emit all edges as LOSER
# else emit all edges as normal (effectively UNKNOWN)

def mark_losers(key,mvalue,mr):
  if WINNER in mvalue:
    vset.append(key)
    return
  flag = 0
  for value in mvalue:
    if value[0] == LOSER:
      flag = 1
      break
  if flag:
    for value in mvalue:
      if key < value[2]: mr.add((key,value[2]),LOSER)
      else: mr.add((value[2],key),LOSER)
  else:
    for value in mvalue:
      if key < value[2]: mr.add((key,value[2]),(value[1],value[3]))
      else: mr.add((value[2],key),(value[3],value[1]))

# print vertices to local file

def output_set(itask,mr):
  for vertex in vset:
    print >>fp,vertex
    mr.add(vertex,None)

# ----------------------------------------------------------------------
# main program

# setup

nprocs = pypar.size()
me = pypar.rank()

if len(sys.argv) != 3:
  if me == 0: print "Syntax: luby.py infile outfile"
  sys.exit()
infile = sys.argv[1]
outfile = sys.argv[2]

mr = mrmpi()
#mr.verbosity(1)

# read input file
# results in ((vi,vj),None)

nedges = mr.map_file_char(nprocs,[infile],"\n",80,fileread)
if me == 0: print nedges,"edges in input file"

# eliminate duplicate edges = both I,J and J,I exist
# results in ((vi,vj),None) with all vi < vj

mr.map_mr(mr,invert_edges)
mr.collate()
nedges = mr.reduce(remove_duplicates)
if me == 0: print nedges,"edges after duplicates removed"

# assign a random # to each vertex
# results in ((vi,vj),(Ri,Rj)) with all vi < vj

mr.map_mr(mr,add_random)
nedges = mr.clone()

# loop until no more edges in graph
# edge_game = competition between 2 vertices in edge
# mark_winners = vertex is a winner if never lost
# mark_losers = neighbors of winners are losers

pypar.barrier()
tstart = pypar.time()

vset = []

while nedges:
  mr.reduce(edge_game)
  mr.collate()
  mr.reduce(mark_winners)
  mr.collate()
  mr.reduce(mark_losers)
  #print "VSET",vset
  nedges = mr.collate()
  if me == 0: print nedges,"edges remaining"

pypar.barrier()
tstop = pypar.time()

# output vset

fp = open(outfile + "." + str(me),"w")
if not fp:
  print "ERROR: Could not open output file"
  sys.exit()
mrset = mrmpi()
nset = mrset.map(nprocs,output_set)
fp.close()
if me == 0: print nset,"vertices in independent set"

# timing data

if me == 0:
  print "%g secs to find independent set on %d procs" % (tstop-tstart,nprocs)

# clean up

mr.destroy()
mrset.destroy()
pypar.finalize()
