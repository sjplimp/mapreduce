#!/usr/local/bin/python

"""
setup.py file for PHISH with system MPI library
"""

from distutils.core import setup, Extension

# list of src files for LAMMPS

libfiles = ["phish.cpp", "hash.cpp"]

phish_library = Extension("_phish",
                          sources = libfiles,
                          define_macros = [("MPICH_IGNORE_CXX_SEEK",1)],
                          # additional libs for MPICH on Linux
                          libraries = ["mpich","rt"],
                          # where to find the MPICH lib on Linux
                          library_dirs = ["/usr/local/lib"],
                          )

setup(name = "phish",
      version = "1Jul11",
      author = "Steve Plimpton",
      author_email = "sjplimp@sandia.gov",
      url = "http://www.sandia.gov/~sjplimp/mapreduce.html",
      description = """PHISH library""",
      py_modules = ["phish"],
      ext_modules = [phish_library]
      )
