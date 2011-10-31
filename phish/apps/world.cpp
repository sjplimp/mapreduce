#include "mpi.h"

#ifdef __cplusplus
extern "C" {
#endif

int mpi_comm_world() {return MPI_COMM_WORLD;}
int mpi_any_tag() {return MPI_ANY_TAG;}
int mpi_any_source() {return MPI_ANY_SOURCE;}
int mpi_sum() {return MPI_SUM;}
int mpi_int() {return MPI_INT;}

#ifdef __cplusplus
}
#endif
