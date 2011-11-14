# Python wrapper on PHISH library via ctypes

import types
from ctypes import *

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

  def input(self,iport,datumfunc,donefunc,reqflag):
    if iport == 0:
      self.datum_caller0 = datumfunc
      self.done_caller0 = donefunc
      self.lib.phish_input(iport,self.datum0_def,self.done0_def,reqflag)
    if iport == 1:
      self.datum_caller1 = datumfunc
      self.done_caller1 = donefunc
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

  def datum0_callback(self):
    self.datum0_caller()

  def done0_callback(self):
    self.done0_caller()

  def datum1_callback(self):
    self.datum1_caller()

  def done1_callback(self):
    self.done1_caller()

  def send(self,oport):
    self.lib.phish_send(oport)

  def send_key(self,oport,key):
    ckey = c_char_p(key)
    self.lib.phish_send_key(oport,ckey,len(key))

  def pack_int(self,ivalue):
    cint = c_int(ivalue)
    self.lib.phish_pack_int(cint)

  def pack_double(self,dvalue):
    cdouble = c_double(dvalue)
    self.lib.phish_pack_double(cdouble)

  def pack_string(self,str):
    cstr = c_char_p(str)
    self.lib.phish_pack_string(cstr)
    

    
  def error(self,str):
    self.lib.phish_error(str)

  def warn(self,str):
    self.lib.phish_warn(str)

  def timer(self):
    return self.lib.phish_timer()
