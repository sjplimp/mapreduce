#!/usr/local/bin/python

"""
setup.py file for PHISH with system MPI library (MPICH)
"""

from distutils.core import setup, Extension

import os
path = os.path.dirname(os.getcwd())

# list of src files for PHISH

libfiles = ["%s/src/phish.cpp" % path, "%s/src/hash.cpp" % path]

phish_library = Extension("_phish",
                          sources = libfiles,
                          define_macros = [("MPICH_IGNORE_CXX_SEEK",1)],
                          # src files for LAMMPS
                          include_dirs = ["../src"],
                          # additional libs for MPICH on Linux
                          libraries = ["mpich","mpl","pthread"],
                          # where to find the MPICH lib on Linux
                          library_dirs = ["/usr/local/lib"],
                          )

setup(name = "phish",
      version = "1Nov11",
      author = "Steve Plimpton",
      author_email = "sjplimp@sandia.gov",
      url = "http://www.sandia.gov/~sjplimp/mapreduce.html",
      description = """PHISH library""",
      py_modules = ["phish"],
      ext_modules = [phish_library]
      )
