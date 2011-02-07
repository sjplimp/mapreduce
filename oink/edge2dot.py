#!/usr/local/bin/python

# Syntax: edge2dot.py indir hostdir
#         indir = file or dir of files of edge ID pairs
#         hostdir = file or dir of files containing "ID hostname"

# convert each file into dot format for graphviz
# undirected graph
# 1st vertex in 1st edge is root vertex, color it red
# edges with root vertex are echoed as-is
# edges without root vertex are colored blue
# ouput files have hostname appended to each infile

import sys,glob,re,os

if len(sys.argv) != 3:
  print "Syntax: edge2dot.py indir hostdir"
  sys.exit()

indir = sys.argv[1]
hostdir = sys.argv[2]

# put ID <-> hostname info in dict

if os.path.isdir(hostdir):
  hostfiles = glob.glob("%s/*" % hostdir)
else: hostfiles = [hostdir]
key2value = {}
for file in hostfiles:
  lines = open(file,"r").readlines()
  for line in lines:
    words = line.split()
    key2value[words[0]] = words[1]

# infiles = list of files to process

if os.path.isdir(indir):
  infiles = glob.glob("%s/*" % indir)
else: infiles = [indir]

for infile in infiles:
  lines = open(infile,"r").readlines()
  root = lines[0].split()[0]
  name = key2value[root]
  fp = open(infile + "." + name,"w")
  print >>fp,'graph %s {\n  "%s" [color=red]' % (name,name)

  for line in lines:
    words = line.split()
    vi = key2value[words[0]]
    vj = key2value[words[1]]
    if root == words[0]:
      print >>fp,'  "%s" -- "%s"' % (vi,vj)
    else:
      print >>fp,'  "%s" -- "%s" [color=green]' % (vi,vj)
  print >>fp,"}"
  fp.close()
