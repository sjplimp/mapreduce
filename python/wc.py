#!/usr/local/bin/python

import sys
#import mpi

from mrmpi import mrmpi

def map(itask,mr,dummy):
  text = open(files[itask]).read()
  words = text.split()
  for word in words: mr.add(word,None)
  #mr.add_multi(words,len(words)*[None])

def mapfile(itask,file,mr,dummy):
  text = open(file).read()
  words = text.split()
  for word in words: mr.add(word,None)
  #mr.add_multi(words,len(words)*[None])

def mapchar(itask,text,mr):
  words = text.split()
  for word in words: mr.add(word,None)
  #mr.add_multi(words,len(words)*[None])

def hash(key,mr):
  return 0

def reduce(key,mvalue,mr):
  mr.add(key,len(mvalue))

def output(key,mvalue,mr):
  count[0] += 1
  if count[0] > count[1]: return
  print key,mvalue

def compare(key1,key2):
  if key1 < key2: return -1
  elif key1 > key2: return 1
  else: return 0

# main program

#me,nprocs = mpi.init()
#print "MPIWORLD",mpi.MPI_COMM_WORLD

if len(sys.argv) < 2:
  print "Syntax: wc.py file1 file2 ..."
  sys.exit()
files = sys.argv[1:]

m = mrmpi()
#m.verbosity(2)

nkey = m.map(len(files),map)
#nkey = m.map_file_list(files[0],mapfile)
#nkey = m.map_file_char(5*len(files),files,'\n',128,mapchar)
#nkey = m.map_file_str(5*len(files),files," ",128,mapchar)

#import copy
#mnew = copy.copy(m)
#del m

nunique = m.collate()
m.reduce(reduce)

#m.sort_keys(compare)
m.collate()

count = [0,10]
m.reduce(output)

print "Nwords,Nunique",nkey,nunique

#mpi.finalize()
