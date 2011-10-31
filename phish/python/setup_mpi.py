#!/usr/local/bin/python

"""
setup.py file for MPICH library
"""

from distutils.core import setup, Extension

# list of src files for MPICH

mpi_library = Extension("_mpi",
                        sources = ["world.cpp"],
                        define_macros = [("MPICH_IGNORE_CXX_SEEK",1)],
                        # additional libs for MPICH on Linux
                        libraries = ["mpich","rt"],
                        # where to find the MPICH lib on Linux
                        library_dirs = ["/usr/local/lib"],
                        )

setup(name = "mpi",
      version = "1Jul11",
      author = "Steve Plimpton",
      author_email = "sjplimp@sandia.gov",
      url = "http://www.sandia.gov/~sjplimp/mapreduce.html",
      description = """MPICH library""",
      py_modules = ["mpi"],
      ext_modules = [mpi_library]
      )
