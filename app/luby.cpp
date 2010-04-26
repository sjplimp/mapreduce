/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#include "mpi.h"
#include "stdlib.h"
#include "string.h"
#include "luby.h"
#include "object.h"
#include "mapreduce.h"
#include "map_vertex_random.h"
#include "reduce_edge_winner.h"
#include "reduce_vertex_winner.h"
#include "reduce_vertex_loser.h"
#include "error.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;

enum{MAPREDUCE,MAP,REDUCE,HASH,COMPARE};   // same as in object.cpp

/* ---------------------------------------------------------------------- */

Luby::Luby(APP *app) : Pointers(app) {}

/* ---------------------------------------------------------------------- */

void Luby::command(int narg, char **arg)
{
  if (narg != 3) error->all("Illegal luby command");

  int me,nprocs;
  MPI_Comm_rank(world,&me);
  MPI_Comm_size(world,&nprocs);

  MapReduce *mr = (MapReduce *) obj->find_object(arg[1],MAPREDUCE);
  if (!mr) error->all("Luby MRMPI for edges is invalid");
  MapReduce *mrv = (MapReduce *) obj->find_object(arg[2],MAPREDUCE);

  if (!mrv) error->all("Luby MRMPI for vertex set is invalid");

  // map and reduce functions
  // vran uses 1st arg = random # seed
  // vwin uses 3rd arg = vertex MR

  mrv->open();

  MapVertexRandom *vran = new MapVertexRandom(app,"vran",1,&arg[0]);
  ReduceEdgeWinner *ewin = new ReduceEdgeWinner(app,"ewin",0,NULL);
  ReduceVertexWinner *vwin = new ReduceVertexWinner(app,"vwin",1,&arg[2]);
  ReduceVertexLoser *vlose = new ReduceVertexLoser(app,"vlose",0,NULL);

  // assign each vertex initially to its own zone

  mr->map(mr,vran->appmap_mr,vran->appptr);
  uint64_t nedge = mr->clone();

  // loop until zones do not change

  MPI_Barrier(world);
  double tstart = MPI_Wtime();

  int niterate = 0;

  while (nedge) {
    niterate++;
    mr->reduce(ewin->appreduce,ewin->appptr);
    mr->collate(NULL);
    mr->reduce(vwin->appreduce,vwin->appptr);
    mr->collate(NULL);
    mr->reduce(vlose->appreduce,vlose->appptr);
    nedge = mr->collate(NULL);
  }

  mrv->close();

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  // clean up

  delete vran;
  delete ewin;
  delete vwin;
  delete vlose;

  if (me == 0)
    printf("%g secs to perform Luby on %d procs in %d iterations\n",
	   tstop-tstart,nprocs,niterate);
}
