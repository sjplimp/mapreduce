#!/usr/local/bin/python

# convert Karl's link files into a graph
# print stats on resulting graph
# optionally output graph as Matrix Market file

# Syntax: link2graph.py switch arg(s) switch arg(s) ...
#         -c
#              convert hashed vertex IDs to integer vertex IDs (1 to N)
#         -e1
#              build graph from 1 value per edge in link file
#         -e2
#              build graph from 2 values per edge in link file
#         -out histofile
#              print vertex out-degree histogramming to histofile
#         -in histofile
#              print vertex in-degree histogramming to histofile
#         -m matrixfile
#              print graph in Matrix Market format with 1/outdegree field
#         -ff file
#              file with list of binary link files to read in (one per line)
#         -f file1 file2 ...
#              binary link files to read in
#              if specified, this must be the last switch used

import sys,copy
from mrmpi import mrmpi

#  fileread1 map() function
#  for each record in file:
#    vertexsize = 8: KV = 1st 8-byte field, 3rd 8-byte field
#    vertexsize = 16: KV = 1st 16-byte field, 2nd 16-byte field
#  output KV: (Vi,Vj)

def fileread(itask,file,mr):
  str = open(file).read()
  i = 0
  while i < len(str):
    mr.add(str[i:i+8],str[i+16:i+24])
    i += 32

#  vertex_emit reduce() function
#  input KMV: (Vi,[Vj])
#  output KV: (Vi,NULL) (Vj,NULL)
#  omit any Vi if first 8 bytes is 0

def vertex_emit(key,mvalue,mr):
  if key != 0: mr.add(key,None);
  if mvalue[0] != 0: mr.add(mvalue[0],None);

#  vertex_unique reduce() function
#  input KMV: (Vi,[NULL NULL ...])
#  output KV: (Vi,NULL)

def vertex_unique(key,mvalue,mr):
  mr.add(key,None);

#  edge_unique reduce() function
#  input KMV: (Vi,[Vj Vk ...])
#  output KV: (Vi,Vj) (Vi,Vk) ...
#  only an edge where first 8 bytes of Vi or Vj are both non-zero is emitted
#  only unique edges are emitted

def edge_unique(key,mvalue,mr):
 if key == 0: return;
 hash = {}
 for value in mvalue:
   if value == 0: continue
   if value in hash: continue
   hash[value] = None;
   mr.add(key,value)

#  vertex_label reduce() function
#  input KMV: (Vi,[])
#  output KV: (Vi,ID), where ID is a unique int from 1 to N

def vertex_label(key,mvalue,mr,label):
  label(1) += 1
  mr.add(key,label(0)+label(1))

#  edge_label1 reduce() function
#  input KMV: (Vi,[Vj Vk ...]), one of the mvalues is a 1-N int ID
#  output KV: (Vj,ID) (Vj,ID) ...

def edge_label1(key,mvalue,mr):

 # id = int ID in mvalue list

 int offset = 0;
 for (int i = 0; i < nvalues; i++)
   if (valuebytes[i] == sizeof(int)) break;
   offset += valuebytes[i];
 int id = - *((int *) &multivalue[offset]);

 offset = 0;
 for (int i = 0; i < nvalues; i++)
   if (valuebytes[i] != sizeof(int))
     kv->add(&multivalue[offset],valuebytes[i],(char *) &id,sizeof(int));
   offset += valuebytes[i];

#  edge_label2 reduce() function
#  input KMV: (Vi,[Vj Vk ...]), one of the mvalues is a positive int = ID
#  output KV: (Vj,ID) (Vj,ID) ...

def edge_label2(key,mvalue,mr):
 // id = negative int in mvalue list

 int *vertex = (int *) multivalue;
 for (i = 0; i < nvalues; i++)
   if (vertex[i] > 0) break;
 int id = vertex[i];

 for (i = 0; i < nvalues; i++)
   if (vertex[i] < 0) {
     vi = -vertex[i];
     kv->add((char *) &vi,sizeof(int),(char *) &id,sizeof(int));

#  edge_count reduce() function
#  input KMV: (Vi,[Vj Vk ...])
#  output KV: (Vj,degree), degree as negative value

def edge_count(key,mvalue,mr)
  mr.add(key,-len(mvalue))

#  edge_reverse reduce() function
#  input KMV: (Vi,[value])
#  output KV: (value,Vi)

def edge_reverse(key,mvalue,mr):
  mr->add(mvalue[0],key)

#  edge_histo reduce() function
#  input KMV: (degree,[Vi Vj ...])
#  output KV: (degree,ncount) where ncount = # of V in multivalue

def edge_histo(key,mvalue,mr)
  mr.add(key,len(mvalue))

#  histo_sort compare() function
#  sort degree values in reverse order

def histo_sort(value1,value2):
 if value1 > value2: return -1;
 else if value1 < value2: return 1;
 else: return 0;

#  histo_write reduce() function
#  input KMV: (degree,count)
#  write pair to file, create no new KV

def histo_write(key,mvalue,mr,ptr):
 FILE *fp = (FILE *) ptr;
 int *degree = (int *) key;
 int *count = (int *) multivalue;
 fprintf(fp,"%d %d\n",*degree,*count);

#  matrix_write reduce() function
#  input KMV: (Vi,[Vj Vk ...]), one of the mvalues is a negative int
#  negative int is degree of Vi
#  write each edge to file, create no new KV

def matrix_write(key,mvalue,mr,ptr):
 FILE *fp = (FILE *) ptr;

 int *vertex = (int *) multivalue;
 for (i = 0; i < nvalues; i++)
   if (vertex[i] < 0) break;
 double inverse_outdegree = -1.0/vertex[i];

 int vi = *((int *) key);

 for (i = 0; i < nvalues; i++)
   if (vertex[i] > 0) 
     fprintf(fp,"%d %d %g\n",vi,vertex[i],inverse_outdegree);

# main program
# parse command-line args

outhfile = None
inhfile = None
mfile = None
convertflag = 0
vertexsize = 8
onefile = None
nfiles = 0
argfiles = None

flag = 0
args = sys.argv[1:]
narg = len(args)
iarg = 0

while iarg < narg:
  if args[iarg] == "-out":
    if iarg+2 > narg: raise StandardError,"Bad args"
    outhfile = args[iarg+1]
    iarg += 2
  elif args[iarg] == "-in":
    if iarg+2 > narg: raise StandardError,"Bad args"
    inhfile = args[iarg+1]
    iarg += 2
  elif args[iarg] == "-m":
    if iarg+2 > narg: raise StandardError,"Bad args"
    mfile = args[iarg+1]
    iarg += 2
  elif args[iarg] == "-c":
    if iarg+2 > narg: raise StandardError,"Bad args"
    convertflag = 1
    iarg += 1
  elif args[iarg] == "-e1":
    if iarg+2 > narg: raise StandardError,"Bad args"
    vertexsize = 8
    iarg += 1
  elif args[iarg] == "-e2":
    if iarg+2 > narg: raise StandardError,"Bad args"
    vertexsize = 16
    iarg += 1
  elif args[iarg] == "-ff":
    if iarg+2 > narg: raise StandardError,"Bad args"
    onefile = args[iarg+1]
    iarg += 2
  elif args[iarg] == "-f":
    onefile = args[iarg+1]
    iarg += 2
    nfiles = narg-1 - iarg; 
    argfiles = args[iarg+1:];
    iarg = narg;
  else:
    flag = 1;
    break;

if flag:
  print "Syntax: link2graph switch arg(s) switch arg(s) ..."
  sys.exit()

if not onefile and not argfiles:
  print "No input files specified"
  sys.exit()

if convertflag == 0 and (outhfile or inhfile):
  print "Must convert vertex values if histogram"
  sys.exit()

if convertflag == 0 and mfile:
  print "Must convert vertex values if output matrix"
  sys.exit()

# mrraw = all edges in file data

mrraw = mrmpi()
mrraw.verbosity(1)
nrawedges = mrraw.map_file_list(onefile,fileread)

# mrvert = unique non-zero vertices

mrvert = copy.copy(mrraw)
mrvert.clone()
mrvert.reduce(vertex_emit)
mrvert.collate();
nverts = mrvert.reduce(vertex_unique)

# mredge = unique I->J edges with I and J non-zero
# no longer need mrraw

mredge = copy.copy(mrraw)
mredge.collate()
nedges = mredge.reduce(edge_unique);
del mrraw;

# set nsingleton to -1 in case never compute it via options

nsingleton = -1;

# update mredge so its vertices are unique ints from 1-N, not hash values

if convertflag:

  # mrvertlabel = vertices with unique IDs 1-N
  # label.nthresh = # of verts on procs < me
  # no longer need mrvert

  MPI_Scan(nverts,nthresh,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
  label = (0,nthresh-nverts)

  mrvertlabel = copy.copy(mrvert)
  mrvertlabel.clone()
  mrvertlabel.reduce(vertex_label,label)
    
  del mrvert

  # reset all vertices in mredge from 1 to N

  mredge.add_kv(mrvertlabel)
  mredge.collate()
  mredge.reduce(edge_label1)
  mredge.add_kv(mrvertlabel)
  mredge.collate()
  mredge.reduce(edge_label2);
  
  del mrvertlabel

# compute and output an out-degree histogram

if outhfile:

  # mrdegree = vertices with their out degree as negative value
  # nsingleton = # of verts with 0 degree

  MapReduce *mrdegree = new MapReduce(*mredge);
  mrdegree->collate(NULL);
  int n = mrdegree->reduce(&edge_count,NULL);
  nsingleton = nverts - n;

  # mrhisto KV = (out degree, vert count)

  MapReduce *mrhisto = mrdegree;
  mrhisto->clone();
  mrhisto->reduce(&edge_reverse,NULL);
  mrhisto->collate(NULL);
  mrhisto->reduce(&edge_histo,NULL);

  # output sorted histogram of out degree
  # add in inferred zero-degree as last entry

  FILE *fp;
  fp = fopen(outhfile,"w");
  fprintf(fp,"Out-degree histogram\n");
  fprintf(fp,"Degree vertex-count\n");

  mrhisto->gather(1);
  mrhisto->sort_keys(&histo_sort);
  mrhisto->clone();
  mrhisto->reduce(&histo_write,fp);
  
  del mrhisto

  fprintf(fp,"%d %d\n",0,nsingleton);
  fclose(fp);

# compute and output an in-degree histogram

if inhfile:

  # mrdegree = vertices with their out degree as negative value
  # nsingleton_in = # of verts with 0 degree

  MapReduce *mrdegree = new MapReduce(*mredge);
  mrdegree->clone();
  mrdegree->reduce(&edge_reverse,NULL);
  mrdegree->collate(NULL);
  int n = mrdegree->reduce(&edge_count,NULL);
  int nsingleton_in = nverts - n;
  
  # mrhisto KV = (out degree, vert count)

  MapReduce *mrhisto = mrdegree;
  mrhisto->clone();
  mrhisto->reduce(&edge_reverse,NULL);
  mrhisto->collate(NULL);
  mrhisto->reduce(&edge_histo,NULL);

  # output sorted histogram of out degree
  # add in inferred zero-degree as last entry

  FILE *fp;
  fp = fopen(inhfile,"w");
  fprintf(fp,"In-degree histogram\n");
  fprintf(fp,"Degree vertex-count\n");

  mrhisto->gather(1);
  mrhisto->sort_keys(&histo_sort);
  mrhisto->clone();
  mrhisto->reduce(&histo_write,fp);

  del mrhisto

  fprintf(fp,"%d %d\n",0,nsingleton_in);
  fclose(fp);

# output a Matrix Market file
# one-line header + one chunk per proc

if mfile:

  # mrdegree = vertices with their out degree as negative value
  # nsingleton = # of verts with 0 degree

  MapReduce *mrdegree = new MapReduce(*mredge);
  mrdegree->collate(NULL);
  int n = mrdegree->reduce(&edge_count,NULL);
  nsingleton = nverts - n;

  char fname[128];
  sprintf(fname,"%s.header",mfile);
  FILE *fp = fopen(fname,"w");
  fprintf(fp,"%d %d %d\n",nverts,nverts,nedges);
  fclose(fp);

  char fname[128];
  sprintf(fname,"%s.%d",mfile,me);
  FILE *fp = fopen(fname,"w");

  # mrout KV = (Vi,[Vj Vk ... Vz outdegree-of-Vi]
  # print out matrix edges in Matrix Market format with 1/out-degree

  MapReduce *mrout = new MapReduce(*mredge);
  mrout->kv->add(mrdegree->kv);
  mrout->collate(NULL);
  mrout->reduce(&matrix_write,fp);

  del mrdegree
  del mrout
  fclose(fp);
  
# clean up
   
del mredge

printf("Graph: %d original edges\n",nrawedges);
printf("Graph: %d unique vertices\n",nverts);
printf("Graph: %d unique edges\n",nedges);
printf("Graph: %d singleton vertices\n",nsingleton);
printf("Time:  %g secs\n",tstop-tstart);

