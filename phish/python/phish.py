# Python wrapper on PHISH library via ctypes

import types
from ctypes import *

# these need to be made visible to Python minnow

PHISH_RAW = 0
PHISH_BYTE = 1
PHISH_INT = 2
PHISH_UINT64 = 3
PHISH_DOUBLE = 4
PHISH_STRING = 5
PHISH_INT_ARRAY = 6
PHISH_UINT64_ARRAY = 7
PHISH_DOUBLE_ARRAY = 8

class Phish:
  def __init__(self):
    try:
      self.lib = CDLL("_phish.so")
    except:
      raise StandardError,"Could not load PHISH dynamic library"

    # setup callbacks

    ALLDONEFUNC = CFUNCTYPE(c_void_p)
    self.alldone_def = ALLDONEFUNC(self.alldone_callback)
    
    DATUMFUNC = CFUNCTYPE(c_void_p,c_int)
    self.datum0_def = DATUMFUNC(self.datum0_callback)
    self.datum1_def = DATUMFUNC(self.datum1_callback)
    DONEFUNC = CFUNCTYPE(c_void_p)
    self.done0_def = DONEFUNC(self.done0_callback)
    self.done1_def = DONEFUNC(self.done1_callback)
    
    PROBEFUNC = CFUNCTYPE(c_void_p)
    self.probe_def = PROBEFUNC(self.probe_callback)
    
  def init(self,args):
    narg = len(args)
    cargs = (c_char_p*narg)(*args)
    n = self.lib.phish_init_python(narg,cargs)
    return args[n:]

  def world(self):
    me = c_int()
    nprocs = c_int()
    world = self.lib.phish_world(byref(me),byref(nprocs))
    return me.value,nprocs.value,world

  def exit(self):
    self.lib.phish_exit()

  # clunky: named callback for each port
    
  def input(self,iport,datumfunc,donefunc,reqflag):
    if iport == 0:
      self.datum0_caller = datumfunc
      self.done0_caller = donefunc
      self.lib.phish_input(iport,self.datum0_def,self.done0_def,reqflag)
    if iport == 1:
      self.datum1_caller = datumfunc
      self.done1_caller = donefunc
      self.lib.phish_input(iport,self.datum1_def,self.done1_def,reqflag)
    
  def output(self,iport):
    self.lib.phish_output(iport)

  def check(self):
    self.lib.phish_check()

  def done(self,donefunc):
    self.alldone_caller = donefunc
    self.lib.phish_done(self.alldone_def)
    
  def alldone_callback(self):
    self.alldone_caller()

  def close(self,iport):
    self.lib.phish_close(iport)

  def loop(self):
    self.lib.phish_loop()

  def probe(self,probefunc):
    self.probe_caller = probefunc
    self.lib.phish_probe(self.probe_def)

  def probe_callback(self):
    self.probe_caller()
    
  def datum0_callback(self,nvalues):
    self.datum0_caller(nvalues)

  def done0_callback(self):
    if self.done0_caller: self.done0_caller()

  def datum1_callback(self,nvalues):
    self.datum1_caller(nvalues)

  def done1_callback(self):
    if self.done1_caller: self.done1_caller()

  def send(self,oport):
    self.lib.phish_send(oport)

  def send_key(self,oport,key):
    ckey = c_char_p(key)
    self.lib.phish_send_key(oport,ckey,len(key))

  # not all datatypes are supported yet for pack
    
  def pack_datum(self,ptr,len):
    self.lib.phish_pack_datum(ptr,len)

  def pack_byte(self,value):
    cchar = c_char(value)
    self.lib.phish_pack_byte(cchar)

  def pack_int(self,value):
    self.lib.phish_pack_int(value)

  def pack_uint64(self,value):
    self.lib.phish_pack_uint64(value)

  def pack_double(self,value):
    cdouble = c_double(value)
    self.lib.phish_pack_double(cdouble)

  def pack_string(self,str):
    cstr = c_char_p(str)
    self.lib.phish_pack_string(cstr)

  def pack_int_array(self,ivec):
    n = len(ivec)
    ptr = (c_int * n)()    # don't understand this syntax
    for i in xrange(n): ptr[i] = ivec[i]
    self.lib.phish_pack_int_array(ptr,n)

  # not all datatypes are supported yet for unpack
    
  def unpack(self):
    buf = c_char_p()
    len = c_int()
    type = self.lib.phish_unpack(byref(buf),byref(len))
    if type == PHISH_BYTE:
      ptr = cast(buf,POINTER(c_char))
      return type,ptr[0],len.value
    if type == PHISH_INT:
      ptr = cast(buf,POINTER(c_int))
      return type,ptr[0],len.value
    if type == PHISH_UINT64:
      ptr = cast(buf,POINTER(c_ulonglong))
      return type,ptr[0],len.value
    if type == PHISH_DOUBLE:
      ptr = cast(buf,POINTER(c_double))
      return type,ptr[0],len.value
    if type == PHISH_STRING:
      return type,buf.value,len.value
    if type == PHISH_INT_ARRAY:
      ptr = cast(buf,POINTER(c_int))
      ivec = len.value * [0]
      for i in xrange(len.value): ivec[i] = ptr[i]
      return type,ivec,len.value
    self.lib.phish_error("Python phish_unpack did not recognize data type")

  def datum(self):
    buf = c_char_p()
    len = c_int()
    type = self.lib.phish_datum(byref(buf),byref(len))
    return buf,len.value
  
  def error(self,str):
    self.lib.phish_error(str)

  def warn(self,str):
    self.lib.phish_warn(str)

  def timer(self):
    return self.lib.phish_timer()
