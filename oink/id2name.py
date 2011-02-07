#!/usr/local/bin/python

# Syntax: id2host.py indir N hostdir
#         indir = file or dir of files of vertex IDs with other info
#         N = column the IDs are in within infile
#         hostdir = file or dir of files containing "ID hostname"

# ouput files have "annotated" appended to each infile

import sys,glob,re,os

if len(sys.argv) != 4:
  print "Syntax: id2host.py indir N hostdir"
  sys.exit()

indir = sys.argv[1]
ncol = int(sys.argv[2]) - 1
hostdir = sys.argv[3]

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
  fp = open(infile + ".annotated","w")
  lines = open(infile,"r").readlines()
  for line in lines:
    words = line.split()
    key = words[ncol]
    value = key2value[key]
    words[ncol] = value
    print >>fp," ".join(words)
  fp.close()
