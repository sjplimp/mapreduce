# Python wrapper on MPI library via ctypes
# works with MPICH, need to test OpenMPI

import types
from ctypes import *

class MPI:
  def __init__(self):
    try:
      self.lib = CDLL("_mpi.so")
    except:
      raise StandardError,"Could not load MPI dynamic library"

  def Init(self,myargs):
    narg = c_int(0)
    args = (c_char_p*1)(None)
    self.lib.MPI_Init(byref(narg),byref(args))

    # MPI constants
    
    COMM_WORLD = c_int()
    COMM_WORLD = self.lib.mpi_comm_world()
    self.COMM_WORLD = COMM_WORLD

    ANY_TAG = c_int()
    ANY_TAG = self.lib.mpi_any_tag()
    self.ANY_TAG = ANY_TAG

    ANY_SOURCE = c_int()
    ANY_SOURCE = self.lib.mpi_any_source()
    self.ANY_SOURCE = ANY_SOURCE
    
    SUM = c_int()
    SUM = self.lib.mpi_sum()
    self.SUM = SUM

    INT = c_int()
    INT = self.lib.mpi_int()
    self.INT = INT

  def Finalize(self):
    self.lib.MPI_Finalize()

  def Comm_rank(self,comm):
    me = c_int()
    self.lib.MPI_Comm_rank(comm,byref(me))
    return me.value

  def Comm_size(self,comm):
    nprocs = c_int()
    self.lib.MPI_Comm_size(comm,byref(nprocs))
    return nprocs.value

  def Bcast(self,n,source,comm):
    foo = c_int(n)
    self.lib.MPI_Bcast(byref(foo),1,self.INT,source,comm)
    n = foo.value
    print "NNN",n
