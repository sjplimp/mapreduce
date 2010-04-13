/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

/* 
   C or Fortran style library interface to APP
   new APP-specific functions can be added
*/

#include "mpi.h"

/* ifdefs allow this file to be included in a C program */

#ifdef __cplusplus
extern "C" {
#endif

void app_open(int, char **, MPI_Comm, void **);
void app_close(void *);
void app_file(void *, char *);
char *app_command(void *, char *);

#ifdef __cplusplus
}
#endif
