# Python wrapper on PHISH library via ctypes

import types
from ctypes import *

# same data type defs as in src/phish.h

RAW = 0
BYTE = 1
INT = 2
UINT64 = 3
DOUBLE = 4
STRING = 5
INT_ARRAY = 6
UINT64_ARRAY = 7
DOUBLE_ARRAY = 8

# load PHISH C++ library

try:
  lib = CDLL("_phish.so")
except:
  raise StandardError,"Could not load PHISH dynamic library"

# function defs
# one-to-one match to functions in src/phish.h

def init(args):
  narg = len(args)
  cargs = (c_char_p*narg)(*args)
  n = lib.phish_init_python(narg,cargs)
  return args[n:]

def school():
  me = c_int()
  nprocs = c_int()
  lib.phish_school(byref(me),byref(nprocs))
  return me.value,nprocs.value

def exit():
  lib.phish_exit()

# clunky: using named callback for each port
# Tim says to use closures to generate callback function code
    
def input(iport,datumfunc,donefunc,reqflag):
  global datum0_caller,done0_caller
  global datum1_caller,done1_caller
  if iport == 0:
    datum0_caller = datumfunc
    done0_caller = donefunc
    if datumfunc and donefunc:
      lib.phish_input(iport,datum0_def,done0_def,reqflag)
    elif datumfunc:
      lib.phish_input(iport,datum0_def,None,reqflag)
    elif donefunc:
      lib.phish_input(iport,None,done0_def,reqflag)
    else:
      lib.phish_input(iport,None,None,reqflag)
  if iport == 1:
    datum1_caller = datumfunc
    done1_caller = donefunc
    if datumfunc and donefunc:
      lib.phish_input(iport,datum1_def,done1_def,reqflag)
    elif datumfunc:
      lib.phish_input(iport,datum1_def,None,reqflag)
    elif donefunc:
      lib.phish_input(iport,None,done1_def,reqflag)
    else:
      lib.phish_input(iport,None,None,reqflag)
    
def output(iport):
  lib.phish_output(iport)

def check():
  lib.phish_check()

def done(donefunc):
  global alldone_caller
  alldone_caller = donefunc
  if donefunc: lib.phish_done(alldone_def)
  else: lib.phish_done(None)

def alldone_callback():
  alldone_caller()

def close(iport):
  lib.phish_close(iport)

def loop():
  lib.phish_loop()

def probe(probefunc):
  global probe_caller
  probe_caller = probefunc
  lib.phish_probe(probe_def)

def probe_callback():
  probe_caller()

def datum0_callback(nvalues):
  datum0_caller(nvalues)

def done0_callback():
  done0_caller()

def datum1_callback(nvalues):
  datum1_caller(nvalues)

def done1_callback():
  done1_caller()

def recv():
  return lib.phish_recv()
  
def send(oport):
  lib.phish_send(oport)

def send_key(oport,key):
  ckey = c_char_p(key)
  lib.phish_send_key(oport,ckey,len(key))

def pack_datum(ptr,len):
  lib.phish_pack_datum(ptr,len)

def pack_raw(str):
  # need non-NULL-terminated byte string
  lib.phish_error("Python wrapper cannot yet pack RAW datum field")
  
def pack_byte(value):
  cchar = c_char(value)
  lib.phish_pack_byte(cchar)

def pack_int(value):
  lib.phish_pack_int(value)

def pack_uint64(value):
  lib.phish_pack_uint64(value)

def pack_double(value):
  cdouble = c_double(value)
  lib.phish_pack_double(cdouble)

def pack_string(str):
  cstr = c_char_p(str)
  lib.phish_pack_string(cstr)

def pack_int_array(vec):
  n = len(vec)
  ptr = (c_int * n)()    # don't understand this syntax
  for i in xrange(n): ptr[i] = vec[i]
  lib.phish_pack_int_array(ptr,n)

def pack_uint64_array(vec):
  n = len(vec)
  ptr = (c_ulonglong * n)()    # don't understand this syntax
  for i in xrange(n): ptr[i] = vec[i]
  lib.phish_pack_uint64_array(ptr,n)

def pack_double_array(vec):
  n = len(vec)
  ptr = (c_double * n)()    # don't understand this syntax
  for i in xrange(n): ptr[i] = vec[i]
  lib.phish_pack_double_array(ptr,n)

def unpack():
  buf = c_char_p()
  len = c_int()
  type = lib.phish_unpack(byref(buf),byref(len))
  if type == RAW:
    # need non-NULL-terminated byte string
    lib.phish_error("Python wrapper cannot yet unpack RAW datum field")
  if type == BYTE:
    ptr = cast(buf,POINTER(c_char))
    return type,ptr[0],len.value
  if type == INT:
    ptr = cast(buf,POINTER(c_int))
    return type,ptr[0],len.value
  if type == UINT64:
    ptr = cast(buf,POINTER(c_ulonglong))
    return type,ptr[0],len.value
  if type == DOUBLE:
    ptr = cast(buf,POINTER(c_double))
    return type,ptr[0],len.value
  if type == STRING:
    return type,buf.value,len.value
  if type == INT_ARRAY:
    ptr = cast(buf,POINTER(c_int))
    vec = len.value * [0]
    for i in xrange(len.value): vec[i] = ptr[i]
    return type,vec,len.value
  if type == UINT64_ARRAY:
    ptr = cast(buf,POINTER(c_ulongulong))
    vec = len.value * [0]
    for i in xrange(len.value): vec[i] = ptr[i]
    return type,vec,len.value
  if type == DOUBLE_ARRAY:
    ptr = cast(buf,POINTER(c_double))
    vec = len.value * [0.0]
    for i in xrange(len.value): vec[i] = ptr[i]
    return type,vec,len.value

def datum():
  buf = c_char_p()
  len = c_int()
  type = lib.phish_datum(byref(buf),byref(len))
  return buf,len.value
  
def error(str):
  lib.phish_error(str)

def warn(str):
  lib.phish_warn(str)

def timer():
  return lib.phish_timer()

# define other module variables

# callback functions

ALLDONEFUNC = CFUNCTYPE(c_void_p)
alldone_def = ALLDONEFUNC(alldone_callback)
    
DATUMFUNC = CFUNCTYPE(c_void_p,c_int)
datum0_def = DATUMFUNC(datum0_callback)
datum1_def = DATUMFUNC(datum1_callback)
DONEFUNC = CFUNCTYPE(c_void_p)
done0_def = DONEFUNC(done0_callback)
done1_def = DONEFUNC(done1_callback)
  
PROBEFUNC = CFUNCTYPE(c_void_p)
probe_def = PROBEFUNC(probe_callback)
