/* ----------------------------------------------------------------------
   MR-MPI = MapReduce-MPI library
   http://www.cs.sandia.gov/~sjplimp/mapreduce.html
   Steve Plimpton, sjplimp@sandia.gov, Sandia National Laboratories

   Copyright (2009) Sandia Corporation.  Under the terms of Contract
   DE-AC04-94AL85000 with Sandia Corporation, the U.S. Government retains
   certain rights in this software.  This software is distributed under 
   the modified Berkeley Software Distribution (BSD) License.

   See the README file in the top-level MapReduce directory.
------------------------------------------------------------------------- */

#include "stdlib.h"
#include "string.h"
#include "stdint.h"
#include "spool.h"
#include "memory.h"
#include "error.h"

using namespace MAPREDUCE_NS;

// allocate space for static class variables and initialize them

uint64_t Spool::rsize = 0;
uint64_t Spool::wsize = 0;
double Spool::rtime = 0.0;
double Spool::wtime = 0.0;

#define ALIGNFILE 512              // same as in mapreduce.cpp
#define PAGECHUNK 16

/* ---------------------------------------------------------------------- */

Spool::Spool(char *memfile, int memsize,
	     Memory *memory_caller, Error *error_caller)
{
  memory = memory_caller;
  error = error_caller;

  int n = strlen(memfile) + 1;
  filename = new char[n];
  strcpy(filename,memfile);
  fileflag = 0;
  fp = NULL;

  pagesize = memsize;
  pages = NULL;
  npage = maxpage = 0;

  nentry = size = 0;
}

/* ---------------------------------------------------------------------- */

Spool::~Spool()
{
  memory->sfree(pages);
  if (fileflag) remove(filename);
  delete [] filename;
}

/* ----------------------------------------------------------------------
   assign memory to Spool before writing or reading
   must be of size memsize for both writing and reading
------------------------------------------------------------------------- */

void Spool::assign(char *memblock)
{
  page = memblock;
}

/* ----------------------------------------------------------------------
   complete the Spool after data has been added to it
------------------------------------------------------------------------- */

void Spool::complete()
{
  create_page();
  write_page();
  fclose(fp);
  fp = NULL;

  npage++;
  nentry = size = 0;

  // set sizes for entire spool file

  nkv = esize = 0;
  for (int ipage = 0; ipage < npage; ipage++) {
    nkv += pages[ipage].nentry;
    esize += pages[ipage].size;
  }

  printf("SP Created %s: %d pages, %u entries, %g Mb\n",
	 filename,npage,nkv,esize/1024.0/1024.0);
}

/* ----------------------------------------------------------------------
   return # of pages and ptr to in-memory page
------------------------------------------------------------------------- */

int Spool::request_info(char **ptr)
{
  *ptr = page;
  return npage;
}

/* ----------------------------------------------------------------------
   ready a page of entries
   caller is looping over data in Spool
------------------------------------------------------------------------- */

int Spool::request_page(int ipage)
{
  read_page(ipage);

  // close file if last request

  if (ipage == npage-1) {
    fclose(fp);
    fp = NULL;
  }

  return pages[ipage].nentry;
}

/* ----------------------------------------------------------------------
   add a single entry
------------------------------------------------------------------------- */

void Spool::add(int nbytes, char *entry)
{
  // page is full, write to disk

  if (size+nbytes > pagesize) {
    create_page();
    write_page();
    npage++;
    nentry = size = 0;

    if (nbytes > pagesize) {
      printf("Spool size/limit: %d %d\n",nbytes,pagesize);
      error->one("Single entry exceeds Spool page size");
    }
  }

  memcpy(&page[size],entry,nbytes);
  size += nbytes;
  nentry++;
}

/* ----------------------------------------------------------------------
   create virtual page entry for in-memory page
------------------------------------------------------------------------- */

void Spool::create_page()
{
  if (npage == maxpage) {
    maxpage += PAGECHUNK;
    pages = (Page *) memory->srealloc(pages,maxpage*sizeof(Page),"SP:pages");
  }

  pages[npage].nentry = nentry;
  pages[npage].size = size;
  pages[npage].filesize = roundup(size,ALIGNFILE);
}

/* ----------------------------------------------------------------------
   write in-memory page to disk
------------------------------------------------------------------------- */

void Spool::write_page()
{
  if (fp == NULL) {
    fp = fopen(filename,"wb");
    if (fp == NULL) error->one("Could not open Spool file for writing");
    fileflag = 1;
  }

  double timestart = MPI_Wtime();
  fwrite(page,pages[npage].filesize,1,fp);
  wtime += MPI_Wtime() - timestart;
  wsize += pages[npage].filesize;
}

/* ----------------------------------------------------------------------
   read ipage from disk
------------------------------------------------------------------------- */

void Spool::read_page(int ipage)
{
  if (fp == NULL) {
    fp = fopen(filename,"rb");
    if (fp == NULL) error->one("Could not open Spool file for reading");
  }

  double timestart = MPI_Wtime();
  fread(page,pages[ipage].filesize,1,fp);
  rtime += MPI_Wtime() - timestart;
  rsize += pages[ipage].filesize;
}

/* ----------------------------------------------------------------------
   round N up to multiple of nalign and return it
------------------------------------------------------------------------- */

int Spool::roundup(int n, int nalign)
{
  if (n % nalign == 0) return n;
  n = (n/nalign + 1) * nalign;
  return n;
}
