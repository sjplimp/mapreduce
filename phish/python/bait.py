#!/usr/local/bin/python

# Syntax: bait.py -switch arg(s) ... < in.script
#         -np P = # of procs
#         -var vname value1 value2 ... = set a variable vname to set of strings 
#         -hostfile filename = list of processors to run on
#         -mpi flavor = flavor of MPI = "mpich" or "openmpi"
# abbrevs: -np can also be -n or -p
#          -var can also be -v
#          -hostfile can also be -h
#          -mpi can also be -m

import sys,re

# print error message and quit

def error(msg):
  print "ERROR:",msg
  sys.exit()

# extract next command from lines of input script
# return command name and args, delete processed lines from lines
# return "" at end of input script
# allow for blank lines, comment char "#", continuation char "&"
# perform variable replacement

def next_command(lines):
  if not lines: return "",[]
  line = ""
  while not line and lines: 
    line = lines.pop(0).strip()
    index = line.find('#')
    if index >= 0: line = line[:index].rstrip()
  if not line: return "",[]

  while line[-1] == '&':
    line = line[:-1]
    if not lines: error("Command %s is incomplete" % line)
    addline = lines.pop(0).strip()
    if not addline: error("Command %s is incomplete" % line)
    index = addline.find('#')
    if index >= 0: addline = addline[:index].rstrip()
    if not addline: error("Command %s is incomplete" % line)
    line += addline
    print line

  pattern = "(\$[^{])"
  matches = re.findall(pattern,line)
  pattern = "(\$\{.+?\})"
  matches += re.findall(pattern,line)
  for match in matches:
    if len(match) == 2: vname = match[1]
    else: vname = match[2:-1]
    if vname not in variables:
      error("Variable %s does not exist" % vname)
    index = line.index(match)
    line = line[:index] + " ".join(variables[vname]) + line[index+len(match):]

  words = line.split()
  return words[0],words[1:]
  
# create a variable via variable command in input script

def variable(args):
  if len(args) < 2: error("Illegal variable command");
  if args[0] in variables: error("Variable %s already in use" % args[0]);
  variables[args[0]] = args[1:]

# set a global setting via command in input script

def set(args):
  global verbosity,makefile
  if len(args) != 2: error("Illegal set command");
  if args[0] == "verbosity":
    verbosity = int(args[1])
  elif args[0] == "makefile":
    makefile = args[1]
  else: error("Unrecognized set parameter %s" % arg[0])

# SP class instantiated by sp command in input script

class sp:
  def __init__(self,args):
    narg = len(args)
    if narg < 2: error("Invalid sp command")
    self.id = args[0]
    self.style = args[1]
    self.args = args[2:]
    self.send = []
    self.recv = []
    ids = [sp.id for sp in sps]
    if self.id in ids: error("SP ID %s already in use" % self.id)

# CONNECT class instantiated by connect command in input script

class connect:
  def __init__(self,args):
    narg = len(args)
    if narg < 3: error("Invalid connect command")
    self.sender = args[0]
    if ':' in self.sender:
      self.sender,self.sendport = self.sender.split(':',1)
    else: self.sendport = 1
    self.style = args[1]
    self.receiver = args[2]
    if ':' in self.receiver:
      self.receiver,self.recvport = self.receiver.split(':',1)
    else: self.recvport = 1

# LAYOUT class instantiated by layout command in input script

class layout:
  def __init__(self,args):
    narg = len(args)
    if narg < 2: error("Invalid layout command")
    self.sp = args[0]
    self.nprocs = int(args[1])

# parse command-line args to override default settings

narg = len(sys.argv)
args = sys.argv

nprocs = 1
variables = {}
hostfile = ""
configfile = "configfile"
mpi = "mpich"

iarg = 1
while iarg < narg:
  if args[iarg] == "-np" or args[iarg] == "-n" or args[iarg] == "-p":
    if iarg+2 > narg: error("Invalid command line args")
    nprocs = int(args[iarg+1])
    iarg += 2
  elif args[iarg] == "-var" or args[iarg] == "-v":
    if iarg+3 > narg: error("Invalid command line args")
    start = stop = iarg+2
    while stop < narg and args[stop][0] != '-': stop += 1
    variables[args[iarg+1]] = args[start:stop]
    iarg += 2+stop-start
  elif args[iarg] == "-hostfile" or args[iarg] == "-h":
    if iarg+2 > narg: error("Invalid command line args")
    hostfile = args[iarg+1]
    iarg += 2
  elif args[iarg] == "-configfile" or args[iarg] == "-c":
    if iarg+2 > narg: error("Invalid command line args")
    configfile = args[iarg+1]
    iarg += 2
  elif args[iarg] == "-mpi" or args[iarg] == "-m":
    if iarg+2 > narg: error("Invalid command line args")
    mpi = args[iarg+1]
    if mpi != "mpich" and mpi != "openmpi": error("Invalid mpi setting");
    iarg += 2
  else: error("Invalid command line args")

# defaults for variables specfied by set command

verbosity = 0
makefile = "Makefile"

# initialize data structures for kids, connects, layouts

sps = []
connects = []
layouts = []

# read and process the input script

lines = sys.stdin.readlines()
while lines:
  command,args = next_command(lines)
  if not command: break
  elif command == "variable": variable(args)
  elif command == "set": set(args)
  elif command == "sp": sps.append(sp(args))
  elif command == "connect": connects.append(connect(args))
  elif command == "layout": layouts.append(layout(args))
  else: error("Unrecognized command %s" % command)

# add fields to each SP based on layout and connect params
# check that connections are consistent with layout
  
spids = [sp.id for sp in sps]
layids = [layout.sp for layout in layouts]

nprocs = 0
for sp in sps:
  if sp.id not in layids: sp.nprocs = 1
  elif layids.count(sp.id) > 1: error("More than one layout for SP ID %s" % sp.id)
  else:
    index = layids.index(sp.id)
    sp.nprocs = layouts[index].nprocs
  sp.procstart = nprocs
  nprocs += sp.nprocs

for iconn,connect in enumerate(connects):
  if connect.sender not in spids:
    error("Unknown connect SP ID %s" % connect.sender)
  if connect.receiver not in spids:
    error("Unknown connect SP ID %s" % connect.receiver)
    
  sendindex = spids.index(connect.sender)
  recvindex = spids.index(connect.receiver)
  sps[sendindex].send.append([sendindex,iconn,recvindex])
  sps[recvindex].recv.append([sendindex,iconn,recvindex])

  npsend = sps[sendindex].nprocs
  nprecv = sps[recvindex].nprocs
  sid = sps[sendindex].id
  rid = sps[recvindex].id
  
  if connect.style == "one2one":
    if npsend != 1 or nprecv != 1:
      error("Connect between %s and %s not consistent with layout" % (sid,rid));
  elif connect.style == "one2many":
    if npsend != 1:
      error("Connect between %s and %s not consistent with layout" % (sid,rid));
  elif connect.style == "one2many/rr":
    if npsend != 1:
      error("Connect between %s and %s not consistent with layout" % (sid,rid));
  elif connect.style == "many2one":
    if nprecv != 1:
      error("Connect between %s and %s not consistent with layout" % (sid,rid));
  elif connect.style == "many2many/one":
    if npsend != nprecv:
      error("Connect between %s and %s not consistent with layout" % (sid,rid));

# build executable SPs using Makefile

# create configfile for flavor of MPI

fp = open("configfile","w")

if mpi == "openmpi":
  if hostfile: print >>fp,"mpirun -hostfile hfile",
  else: print >>fp,"mpirun",

for isp,sp in enumerate(sps):
  procstr = "-n %d %s" % (sp.nprocs,sp.style)
  recv = sp.recv
  recvstr = "-recv %d" % len(recv)
  for partner in recv:
    recvstr += " %s %d %d %d %d %d %d" % \
        (connects[partner[1]].style,
         sps[partner[0]].nprocs,sps[partner[0]].procstart,
         int(connects[partner[1]].sendport),
         sp.nprocs,sp.procstart,int(connects[partner[1]].recvport))
  send = sp.send
  sendstr = "-send %d" % len(send)
  for partner in send:
    sendstr += " %s %d %d %d %d %d %d" % \
        (connects[partner[1]].style,
         sp.nprocs,sp.procstart,int(connects[partner[1]].sendport),
         sps[partner[2]].nprocs,sps[partner[2]].procstart,
         int(connects[partner[1]].recvport))
  launchstr = procstr + " " + recvstr + " " + sendstr
  if sp.args: launchstr += " -args " + " ".join(sp.args)
  if mpi == "mpich":
    print >>fp,launchstr
  elif mpi == "openmpi":
    print >>fp,launchstr,
    if isp < len(sps)-1: print >>fp,":",
    else: print >>fp
  
fp.close()

# print stats

print "# of SPs =",len(sps)
print "# of processes =",nprocs

# prompt for how to invoke launch script for flavor of MPI

if mpi == "mpich":
  print "mpiexec -configfile %s" % configfile
elif mpi == "openmpi":
  print "invoke %s from shell" % configfile
