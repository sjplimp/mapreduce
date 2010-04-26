/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#include "mpi.h"
#include "stdlib.h"
#include "string.h"
#include "cc.h"
#include "object.h"
#include "mapreduce.h"
#include "map_vertex_self.h"
#include "map_edge_vertex.h"
#include "map_invert.h"
#include "reduce_edge_zone.h"
#include "reduce_zone_winner.h"
#include "reduce_zone_reassign.h"
#include "error.h"

#include "mapreduce.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;

enum{MAPREDUCE,MAP,REDUCE,HASH,COMPARE};   // same as in object.cpp

/* ---------------------------------------------------------------------- */

CC::CC(APP *app) : Pointers(app) {}

/* ---------------------------------------------------------------------- */

void CC::command(int narg, char **arg)
{
  if (narg != 4) error->all("Illegal cc command");

  int me,nprocs;
  MPI_Comm_rank(world,&me);
  MPI_Comm_size(world,&nprocs);

  MapReduce *mre = (MapReduce *) obj->find_object(arg[1],MAPREDUCE);
  if (!mre) error->all("CC MRMPI for edges is invalid");
  MapReduce *mrv = (MapReduce *) obj->find_object(arg[2],MAPREDUCE);
  if (!mrv) error->all("CC MRMPI for vertices is invalid");
  MapReduce *mr = (MapReduce *) obj->find_object(arg[3],MAPREDUCE);
  if (!mr) error->all("CC MRMPI for zones is invalid");

  // map and reduce functions

  MapVertexSelf *vs = new MapVertexSelf(app,"vs",1,&arg[0]);
  MapEdgeVertex *ev = new MapEdgeVertex(app,"ev",0,NULL);
  ReduceEdgeZone *ezone = new ReduceEdgeZone(app,"ezone",0,NULL);
  ReduceZoneWinner *zwin = new ReduceZoneWinner(app,"zwin",0,NULL);
  MapInvert *inv = new MapInvert(app,"invert",0,NULL);
  ReduceZoneReassign *zr = new ReduceZoneReassign(app,"zr",0,NULL);

  // assign each vertex initially to its own zone

  mrv->map(nprocs,vs->appmap,vs->appptr);

  // loop until zones do not change

  MPI_Barrier(world);
  double tstart = MPI_Wtime();

  int niterate = 0;

  while (1) {
    niterate++;

    mr->map(mre,ev->appmap_mr,ev->appptr);
    mr->add(mrv);
    mr->collate(NULL);
    mr->reduce(ezone->appreduce,ezone->appptr);

    mr->collate(NULL);
    zwin->flag = 0;
    mr->reduce(zwin->appreduce,zwin->appptr);
    int flagall;
    MPI_Allreduce(&zwin->flag,&flagall,1,MPI_INT,MPI_SUM,world);
    if (!flagall) break;

    mrv->map(mrv,inv->appmap_mr,inv->appptr);
    mrv->add(mr);
    mrv->collate(NULL);
    mrv->reduce(zr->appreduce,zr->appptr);
  }

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  // clean up

  delete vs;
  delete ev;
  delete ezone;
  delete zwin;
  delete inv;
  delete zr;

  if (me == 0)
    printf("%g secs to find CCs on %d procs in %d iterations\n",
	   tstop-tstart,nprocs,niterate);
}
