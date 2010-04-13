/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#include "mpi.h"
#include "stdlib.h"
#include "string.h"
#include "cc2.h"
#include "object.h"
#include "mapreduce.h"
#include "map_vertex_self.h"
#include "map_edge_vertex.h"
#include "map_invert_multi.h"
#include "map_zone_multi.h"
#include "map_strip.h"
#include "reduce_edge_zone.h"
#include "reduce_zone_winner.h"
#include "reduce_zone_reassign2.h"
#include "error.h"

#include "mapreduce.h"

using namespace APP_NS;
using MAPREDUCE_NS::MapReduce;

enum{MAPREDUCE,MAP,REDUCE,HASH,COMPARE};   // same as in object.cpp

/* ---------------------------------------------------------------------- */

CC2::CC2(APP *app) : Pointers(app) {}

/* ---------------------------------------------------------------------- */

void CC2::command(int narg, char **arg)
{
  if (narg != 5) error->all("Illegal cc command");

  int me,nprocs;
  MPI_Comm_rank(world,&me);
  MPI_Comm_size(world,&nprocs);

  MapReduce *mre = (MapReduce *) obj->find_object(arg[2],MAPREDUCE);
  if (!mre) error->all("CC MRMPI for edges is invalid");
  MapReduce *mrv = (MapReduce *) obj->find_object(arg[3],MAPREDUCE);
  if (!mrv) error->all("CC MRMPI for vertices is invalid");
  MapReduce *mr = (MapReduce *) obj->find_object(arg[4],MAPREDUCE);
  if (!mr) error->all("CC MRMPI for zones is invalid");

  // map and reduce functions
  // vs uses 1st arg = # of vertices
  // zr uses 2nd arg = threshhold vertex count before splitting zone

  MapVertexSelf *vs = new MapVertexSelf(app,"vs",1,&arg[0]);
  MapEdgeVertex *ev = new MapEdgeVertex(app,"ev",0,NULL);
  ReduceEdgeZone *ezone = new ReduceEdgeZone(app,"ezone",0,NULL);
  ReduceZoneWinner *zwin = new ReduceZoneWinner(app,"zwin",0,NULL);
  MapInvertMulti *im = new MapInvertMulti(app,"im",0,NULL);
  MapZoneMulti *zm = new MapZoneMulti(app,"zm",0,NULL);
  ReduceZoneReassign2 *zr2 = 
    new ReduceZoneReassign2(app,"zr",1,&arg[1]);
  MapStrip *strip = new MapStrip(app,"strip",0,NULL);

  // assign each vertex initially to its own zone

  mrv->map(nprocs,vs->appmap,vs->appptr);

  //if (me == 0) printf("VERTEX ZONE pre-list:\n");
  //mrv->print(-1,1,2,2);
  //if (me == 0) printf("EDGE pre-list:\n");
  //mre->print(-1,1,7,2);

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

    //if (me == 0) printf("EDGE ZONE list:\n");
    //mr->print(-1,1,7,2);

    mr->collate(NULL);
    zwin->flag = 0;
    uint64_t foo = mr->reduce(zwin->appreduce,zwin->appptr);
    int flagall;
    MPI_Allreduce(&zwin->flag,&flagall,1,MPI_INT,MPI_SUM,world);
    if (!flagall) break;

    //if (me == 0) printf("ZONE ZONE list:\n");
    //mr->print(-1,1,2,2);

    mrv->map(mrv,im->appmap_mr,im->appptr);
    mrv->map(mr,zm->appmap_mr,zm->appptr,1);
    mrv->collate(NULL);
    mrv->reduce(zr2->appreduce,zr2->appptr);
    
    //if (me == 0) printf("VERTEX ZONE list:\n");
    //mrv->print(-1,1,2,2);
  }

  // strip any hi-bits from final (Vi,Zi) key/values

  mrv->map(mrv,strip->appmap_mr,strip->appptr);

  //if (me == 0) printf("VERTEX ZONE final list:\n");
  //mrv->print(-1,1,2,2);

  MPI_Barrier(world);
  double tstop = MPI_Wtime();

  // clean up

  delete vs;
  delete ev;
  delete ezone;
  delete zwin;
  delete im;
  delete zm;
  delete zr2;
  delete strip;

  if (me == 0)
    printf("%g secs to find CCs on %d procs in %d iterations\n",
	   tstop-tstart,nprocs,niterate);
}
