#!/usr/local/bin/python

"""
setup.py file for MapReduce MPI files
"""

from distutils.core import setup, Extension

import glob
libfiles = glob.glob("src/*.cpp")

mrmpi_module = Extension("_mrmpi",
                         sources = libfiles,
                         define_macros = [("MPICH_IGNORE_CXX_SEEK",1)],
                         include_dirs = ["src"],
                         library_dirs = ["/usr/local/lib"],
                         libraries = ["mpich","rt"],
                         )

setup (name = 'mrmpi',
       version = '1.0',
       author      = "Steve Plimpton",
       description = """MapReduce MPI library""",
       ext_modules = [mrmpi_module]
       )
