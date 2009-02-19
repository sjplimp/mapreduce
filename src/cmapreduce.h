/* ----------------------------------------------------------------------
   MR = MapReduce library
   Steve Plimpton, sjplimp@sandia.gov, http://cs.sandia.gov/~sjplimp,
   Sandia National Laboratories
   This software is distributed under the lesser GNU Public License (LGPL)
   See the README file in the top-level MapReduce directory for more info
------------------------------------------------------------------------- */

/* C or Fortran style interface to MapReduce library */
/* ifdefs allow this file to be included in a C program */

#include "mpi.h"

#ifdef __cplusplus
extern "C" {
#endif

void *MR_create(MPI_Comm comm);
void *MR_copy(void *MRptr);
void MR_destroy(void *MRptr);

int MR_aggregate(void *MRptr, int (*myhash)(char *, int));
int MR_clone(void *MRptr);
int MR_collapse(void *MRptr, char *key, int keybytes);
int MR_collate(void *MRptr, int (*myhash)(char *, int));
int MR_compress(void *MRptr, 
		void (*mycompress)(char *, int, char *,
				   int, int *, void *KVptr, void *APPptr),
		void *APPptr);
int MR_convert(void *MRptr);
int MR_gather(void *MRptr, int numprocs);
int MR_map(void *MRptr, int nmap,
	   void (*mymap)(int, void *KVptr, void *APPptr),
	   void *APPptr);
int MR_map_add(void *MRptr, int nmap,
	       void (*mymap)(int, void *KVptr, void *APPptr),
	       void *APPptr, int addflag);
int MR_file_list(void *MRptr, char *file,
		 void (*mymap)(int, char *, void *KVptr, void *APPptr),
		 void *APPptr);
int MR_map_file_list_add(void *MRptr, char *file,
			 void (*mymap)(int, char *, void *KVptr, void *APPptr),
			 void *APPptr, int addflag);
int MR_map_file_char(void *MRptr, int nmap, int nfiles, char **files,
		     char sepchar, int delta,
		     void (*mymap)(int, void *KVptr, void *APPptr),
		     void *APPptr);
int MR_map_file_char_add(void *MRptr, int nmap, int nfiles, char **files,
			 char sepchar, int delta,
			 void (*mymap)(int, void *KVptr, void *APPptr),
			 void *APPptr, int addflag);
int MR_map_file_str(void *MRptr, int nmap, int nfiles, char **files,
		    char *sepstr, int delta,
		    void (*mymap)(int, void *KVptr, void *APPptr),
		    void *APPptr);
int MR_map_file_str_add(void *MRptr, int nmap, int nfiles, char **files,
			char *sepstr, int delta,
			void (*mymap)(int, void *KVptr, void *APPptr),
			void *APPptr, int addflag);
int MR_reduce(void *MRptr,
	      void (*myreduce)(char *, int, char *,
			       int, int *, void *KVptr, void *APPptr),
	      void *APPptr);
int MR_scrunch(void *MRptr, int numprocs, char *key, int keybytes);
int MR_sort_keys(void *MRptr, 
		 int (*mycompare)(char *, int, char *, int));
int MR_sort_values(void *MRptr,
		   int (*mycompare)(char *, int, char *, int));
int MR_sort_multivalues(void *MRptr,
			int (*mycompare)(char *, int, char *, int));

void MR_kv_add(void *KVptr, char *key, int keybytes, 
	       char *value, int valuebytes);
void MR_kv_add_multi_static(void *KVptr, int n,
			    char *key, int keybytes,
			    char *value, int valuebytes);
void MR_kv_add_multi_dynamic(void *KVptr, int n,
			     char *key, int *keybytes,
			     char *value, int *valuebytes);
void MR_kv_stats(void *MRptr, int level);
void MR_kmv_stats(void *MRptr, int level);

void MR_set_mapstyle(void *MRptr, int value);
void MR_set_verbosity(void *MRptr, int value);

#ifdef __cplusplus
}
#endif
