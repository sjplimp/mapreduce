# Python wrapper on PHISH library via ctypes

import types
from ctypes import *

class Phish:
  def __init__(self):
    try:
      self.lib = CDLL("_phish.so")
    except:
      raise StandardError,"Could not load PHISH dynamic library"

    self.DONE = 99
    self.DATUM = 100
    self.PROBE = 101
    
    DONEFUNC = CFUNCTYPE(c_void_p)
    self.done_def = DONEFUNC(self.done_callback)
    DATUMFUNC = CFUNCTYPE(c_void_p,c_int)
    self.datum_def = DATUMFUNC(self.datum_callback)
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

    

    
  def done(self,donefunc):
    self.done_callback = donefunc
    self.lib.phish_callback(self.done_def)



    
  def callback_datum(self,datumfunc):
    self.datum_caller = func
    self.lib.phish_callback(self.datum_def)

  def callback_probe(self,func):
    self.probe_caller = func
    self.lib.phish_callback(self.probe_def)
      
  def loop(self):
    self.lib.phish_loop()

  def done_callback(self):
    self.done_caller()

  def datum_callback(self,nvalues):
    self.datum_caller(nvalues)

  def probe_callback(self):
    self.probe_caller()



    
  def pack_int(self,value):
    cint = c_int(value)
    self.lib.phish_pack_int(cint)

  def pack_double(self,value):
    cdouble = c_double(value)
    self.lib.phish_pack_double(cdouble)

  def pack_string(self,value):
    cstr = c_char_p(value)
    self.lib.phish_pack_string(cstr)

  def send(self):
    self.lib.phish_send()

  def send_key(self,key):
    ckey = c_char_p(key)
    self.lib.phish_send_key(ckey,len(key)+1)

  def send_done(self):
    self.lib.phish_send_done()



    
  def error(self,str):
    self.lib.phish_error(str)

  def warn(self,str):
    self.lib.phish_warn(str)

  def timer(self):
    return self.lib.phish_timer()
