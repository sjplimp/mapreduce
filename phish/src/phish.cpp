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

// MPI-based version of PHISH library

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "stdint.h"
#include "phish.h"
#include "hash.h"

/* ---------------------------------------------------------------------- */
// definitions

//#define PHISH_SAFE_SEND 1      // uncomment for safer/slower MPI_Ssend()

enum{SINGLE,PAIRED,HASHED,ROUNDROBIN,CHAIN,RING};     // connection styles
enum{UNUSED_PORT,OPEN_PORT,CLOSED_PORT};              // port status

#define MAXBUF 1024*1024               // max datum length
#define MAXPORT 16                     // max # of input or output ports

typedef void (DatumFunc)(int);         // callback prototypes
typedef void (DoneFunc)();

/* ---------------------------------------------------------------------- */
// variables local to single PHISH instance

MPI_Comm world;           // MPI communicator
int me,nprocs;            // MPI rank and total # of procs

int initflag;             // 1 if phish_init has been called
int checkflag;            // 1 if phish_check has been called

char *appname;            // name of app executable
char *appid;              // ID of app via minnow command
int appcount;             // # of instances of this app via layout command
int appprev;              // # of processes launched prior to this app

// input ports
// each can have multiple connections from output ports of other apps

struct InConnect {        // inbound connection from output port of another app
  int style;              // SINGLE, HASHED, etc
  int nsend;              // # of procs that send to me on this connection
};

struct InputPort {        // one input port
  int status;             // UNUSED or OPEN or CLOSED
  int donecount;          // # of done messages received on this port
  int donemax;            // # of done messages that will close this port
  int nconnect;           // # of connections to this port
  InConnect *connects;    // list of connections
  DatumFunc *datumfunc;   // callback when receive datum on this port
  DoneFunc *donefunc;     // callback when this port closes
};

InputPort *inports;       // list of input ports
int ninports;             // # of used input ports
int donecount;            // # of closed input ports
DoneFunc *alldonefunc;    // callback when all input ports closed

// output ports
// each can have multiple connections to input ports of other apps

struct OutConnect {        // outbound connection to input port of another app
  int style;               // SINGLE, HASHED, etc
  int nrecv;               // # of procs that receive from me
  int recvone;             // single proc ID I send to (nrecv = 1)
  int recvfirst;           // 1st proc ID I send to (nrecv > 1)
  int offset;              // offset from 1st proc for roundrobin
  int recvport;            // port to send to on receivers
};

struct OutPort {           // one output port
  int status;              // UNUSED or OPEN or CLOSED
  int nconnect;            // # of connections from this port
  OutConnect *connects;    // list of connections
};

OutPort *outports;         // list of output ports
int noutports;             // # of used output ports

// send/receive buffers that hold a datum

char *sbuf;                // buffer to hold datum to send
int nsbytes;               // total size of send datum
char *sptr;                // ptr to current loc in sbuf for packing
int npack;                 // # of fields packed thus far into sbuf

char *rbuf;                // buffer to hold received datum
int nrbytes;               // total size of received datum
int nrfields;              // # of fields in received datum
char *rptr;                // ptr to current loc in rbuf for unpacking
int nunpack;               // # of fields unpacked thus far from rbuf

// stats

uint64_t rcount;           // # of datums received
uint64_t scount;           // # of datums sent

// local function prototypes

void send(OutConnect *);
void stats();

/* ---------------------------------------------------------------------- */

void phish_init(int *pnarg, char ***pargs)
{
  initflag = 1;
  checkflag = 0;

  MPI_Init(pnarg,pargs);

  world = MPI_COMM_WORLD;
  MPI_Comm_rank(world,&me);
  MPI_Comm_size(world,&nprocs);

  // memory allocation/initialization for ports and datum buffers

  inports = new InputPort[MAXPORT];
  for (int i = 0; i < MAXPORT; i++) {
    inports[i].status = UNUSED_PORT;
    inports[i].donecount = 0;
    inports[i].donemax = 0;
    inports[i].nconnect = 0;
    inports[i].connects = NULL;
  }
  alldonefunc = NULL;

  outports = new OutPort[MAXPORT];
  for (int i = 0; i < MAXPORT; i++) {
    outports[i].status = UNUSED_PORT;
    outports[i].nconnect = 0;
    outports[i].connects = NULL;
  }

  sbuf = (char *) malloc(MAXBUF*sizeof(char));
  rbuf = (char *) malloc(MAXBUF*sizeof(char));

  if (!sbuf || !rbuf) phish_error("Malloc of datum buffers failed");

  // parse input args and setup communication port data structs

  char **args = *pargs;
  int narg = *pnarg;
  int argstart = narg;
  
  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-app") == 0) {
      int n = strlen(args[iarg+1]) + 1;
      appname = new char[n];
      strcpy(appname,args[iarg+1]);
      n = strlen(args[iarg+2]) + 1;
      appid = new char[n];
      strcpy(appid,args[iarg+2]);
      appcount = atoi(args[iarg+3]);
      appprev = atoi(args[iarg+4]);
      iarg += 5;

    } else if (strcmp(args[iarg],"-in") == 0) {
      int style;
      int sprocs,sfirst,sport,rprocs,rfirst,rport;

      sprocs = atoi(args[iarg+1]);
      sfirst = atoi(args[iarg+2]);
      sport = atoi(args[iarg+3]);
      if (strcmp(args[iarg+4],"single") == 0) style = SINGLE;
      else if (strcmp(args[iarg+4],"paired") == 0) style = PAIRED;
      else if (strcmp(args[iarg+4],"hashed") == 0) style = HASHED;
      else if (strcmp(args[iarg+4],"roundrobin") == 0) style = ROUNDROBIN;
      else if (strcmp(args[iarg+4],"chain") == 0) style = CHAIN;
      else if (strcmp(args[iarg+4],"ring") == 0) style = RING;
      else phish_error("Unrecognized in style");
      rprocs = atoi(args[iarg+5]);
      rfirst = atoi(args[iarg+6]);
      rport = atoi(args[iarg+7]);

      if (rport > MAXPORT)
	phish_error("Invalid input port ID in phish_init");
      InputPort *ip = &inports[rport];
      ip->status = CLOSED_PORT;
      ip->nconnect++;
      ip->connects = (InConnect *) 
	realloc(ip->connects,ip->nconnect*sizeof(InConnect));
      InConnect *ic = &ip->connects[ip->nconnect-1];

      ic->nsend = sprocs;
      ic->style = style;

      switch (style) {
      case SINGLE:
      case HASHED:
      case ROUNDROBIN:
	ip->donemax += sprocs;
	break;
      case PAIRED:
      case CHAIN:
      case RING:
	ip->donemax++;
	break;
      }

      iarg += 8;

    } else if (strcmp(args[iarg],"-out") == 0) {
      int style;
      int sprocs,sfirst,sport,rprocs,rfirst,rport;

      sprocs = atoi(args[iarg+1]);
      sfirst = atoi(args[iarg+2]);
      sport = atoi(args[iarg+3]);
      if (strcmp(args[iarg+4],"single") == 0) style = SINGLE;
      else if (strcmp(args[iarg+4],"paired") == 0) style = PAIRED;
      else if (strcmp(args[iarg+4],"hashed") == 0) style = HASHED;
      else if (strcmp(args[iarg+4],"roundrobin") == 0) style = ROUNDROBIN;
      else if (strcmp(args[iarg+4],"chain") == 0) style = CHAIN;
      else if (strcmp(args[iarg+4],"ring") == 0) style = RING;
      else phish_error("Unrecognized out style");
      rprocs = atoi(args[iarg+5]);
      rfirst = atoi(args[iarg+6]);
      rport = atoi(args[iarg+7]);

      if (sport > MAXPORT)
	phish_error("Invalid output port ID in phish_init");
      OutPort *op = &outports[sport];
      op->status = CLOSED_PORT;
      op->nconnect++;
      op->connects = (OutConnect *) 
	realloc(op->connects,op->nconnect*sizeof(OutConnect));
      OutConnect *oc = &op->connects[op->nconnect-1];

      oc->recvport = rport;
      oc->style = style;

      switch (style) {
      case SINGLE:
	oc->nrecv = 1;
	oc->recvone = rfirst;
	oc->recvfirst = -1;
	oc->offset = -1;
	break;
      case PAIRED:
	oc->nrecv = 1;
	oc->recvone = rfirst + me - sfirst;
	oc->recvfirst = -1;
	oc->offset = -1;
	break;
      case HASHED:
	oc->nrecv = rprocs;
	oc->recvone = -1;
	oc->recvfirst = rfirst;
	oc->offset = -1;
	break;
      case ROUNDROBIN:
	oc->nrecv = rprocs;
	oc->recvone = -1;
	oc->recvfirst = rfirst;
	oc->offset = 0;
	break;
      case CHAIN:
	oc->nrecv = 1;
	oc->recvone = me + 1;
	if (me-sfirst == sprocs-1) {
	  oc->nrecv = 0;
	  oc->recvone = -1;
	}
	oc->recvfirst = -1;
	oc->offset = -1;
	break;
      case RING:
	oc->nrecv = 1;
	oc->recvone = me + 1;
	if (me-sfirst == sprocs-1) oc->recvone = rfirst;
	oc->recvfirst = -1;
	oc->offset = -1;
	break;
      }

      iarg += 8;

    } else if (strcmp(args[iarg],"-args") == 0) {
      argstart = iarg+1;
      iarg = narg;

    } else phish_error("Invalid command-line args in phish_init");
  }

  // strip off PHISH args, leaving app args for app to use

  *pnarg = narg-argstart;
  if (*pnarg > 0) *pargs = &args[argstart];
  else *pargs = NULL;

  // setup send buffer for initial datum

  sptr = sbuf + sizeof(int);
  npack = 0;
}

/* ---------------------------------------------------------------------- */

int phish_init_python(int narg, char **args)
{
  int narg_start = narg;
  phish_init(&narg,&args);
  return narg_start-narg;
}

/* ---------------------------------------------------------------------- */

void phish_school(int *pme, int *pnprocs)
{
  if (!initflag) phish_error("Phish_init has not been called");

  *pme = me;
  *pnprocs = nprocs;
}

/* ---------------------------------------------------------------------- */

void phish_exit()
{
  if (!initflag) phish_error("Phish_init has not been called");
  if (!checkflag) phish_error("Phish_check has not been called");

  // generate stats

  stats();

  // warn if any input port is still open

  for (int i = 0; i < MAXPORT; i++)
    if (inports[i].status == OPEN_PORT)
      phish_warn("Exiting with input port still open");

  // close all output ports

  for (int i = 0; i < MAXPORT; i++) phish_close(i);

  // free datum buffers

  free(sbuf);
  free(rbuf);

  // free port memory

  for (int i = 0; i < MAXPORT; i++)
    if (inports[i].status != UNUSED_PORT) free(inports[i].connects);
  delete [] inports;
  for (int i = 0; i < MAXPORT; i++)
    if (outports[i].status != UNUSED_PORT) free(outports[i].connects);
  delete [] outports;

  // free other PHISH memory

  delete [] appname;
  delete [] appid;

  // shut-down MPI

  MPI_Finalize();
  initflag = checkflag = 0;
}

/* ----------------------------------------------------------------------
   setup single input port iport
   reqflag = 1 if port must be used by input script
------------------------------------------------------------------------- */

void phish_input(int iport, void (*datumfunc)(int), 
		 void (*donefunc)(), int reqflag)
{
  if (!initflag) phish_error("Phish_init has not been called");
  if (checkflag) phish_error("Phish_check has already been called");

  if (iport < 0 || iport > MAXPORT)
    phish_error("Invalid port ID in phish_input");

  if (reqflag && inports[iport].status == UNUSED_PORT)
    phish_error("Input script does not use a required input port");

  if (inports[iport].status == UNUSED_PORT) return;
  inports[iport].status = OPEN_PORT;
  inports[iport].datumfunc = datumfunc;
  inports[iport].donefunc = donefunc;
}

/* ----------------------------------------------------------------------
   setup single output port iport
   no reqflag setting, since script does not have to use the port
------------------------------------------------------------------------- */

void phish_output(int iport)
{
  if (!initflag) phish_error("Phish_init has not been called");
  if (checkflag) phish_error("Phish_check has already been called");

  if (iport < 0 || iport > MAXPORT)
    phish_error("Invalid port count in phish_output");

  if (outports[iport].status == UNUSED_PORT) return;
  outports[iport].status = OPEN_PORT;
}

/* ----------------------------------------------------------------------
   check consistency of input args with ports setup by phish input/output
------------------------------------------------------------------------- */

void phish_check()
{
  if (!initflag) phish_error("Phish_init has not been called");
  if (checkflag) phish_error("Phish_check has already been called");
  checkflag = 1;

  // args processed by phish_init() requested various input ports
  // flagged them as CLOSED, others as UNUSED
  // phish_input() reset CLOSED ports to OPEN
  // error if a port is CLOSED, since phish_input was not called
  // set ninports = # of used input ports
  // initialize donecount before datum exchanges begin

  ninports = 0;
  for (int i = 0; i < MAXPORT; i++) {
    if (inports[i].status == CLOSED_PORT)
      phish_error("Input script uses an undefined input port");
    if (inports[i].status == OPEN_PORT) ninports++;
  }
  donecount = 0;

  // args processed by phish_init() requested various output ports
  // flagged them as CLOSED, others as UNUSED
  // phish_output() reset CLOSED ports to OPEN
  // error if a port is CLOSED, since phish_output was not called
  // set noutports = # of used output ports

  noutports = 0;
  for (int i = 0; i < MAXPORT; i++) {
    if (outports[i].status == CLOSED_PORT)
      phish_error("Input script uses an undefined output port");
    if (outports[i].status == OPEN_PORT) noutports++;
  }

  // stats

  rcount = scount = 0;
}

/* ----------------------------------------------------------------------
   set callback function to invoke when all input ports are closed
------------------------------------------------------------------------- */

void phish_done(void (*donefunc)())
{
  if (!initflag) phish_error("Phish_init has not been called");

  alldonefunc = donefunc;
}

/* ----------------------------------------------------------------------
   close output port iport
------------------------------------------------------------------------- */

void phish_close(int iport)
{
  if (!checkflag) phish_error("Phish_check has not been called");

  if (iport < 0 || iport >= MAXPORT)
    phish_error("Invalid port ID for phish_close");
  OutPort *op = &outports[iport];
  if (op->status != OPEN_PORT) return;

  // loop over connections
  // loop over all receivers in connection
  // send each receiver a done message to its appropriate input port

  for (int iconnect = 0; iconnect < op->nconnect; iconnect++) {
    OutConnect *oc = &op->connects[iconnect];
    int tag = MAXPORT + oc->recvport;
    switch (oc->style) {

    case SINGLE:
    case PAIRED:
    case RING:
#ifdef PHISH_SAFE_SEND
      MPI_Ssend(NULL,0,MPI_BYTE,oc->recvone,tag,world);
#else
      MPI_Send(NULL,0,MPI_BYTE,oc->recvone,tag,world);
#endif
      break;

    case HASHED:
    case ROUNDROBIN:
      for (int i = 0; i < oc->nrecv; i++)
#ifdef PHISH_SAFE_SEND
        MPI_Ssend(NULL,0,MPI_BYTE,oc->recvfirst+i,tag,world);
#else
        MPI_Send(NULL,0,MPI_BYTE,oc->recvfirst+i,tag,world);
#endif
      break;

    case CHAIN:
      if (oc->nrecv) {
#ifdef PHISH_SAFE_SEND
	MPI_Ssend(NULL,0,MPI_BYTE,oc->recvone,tag,world);
#else
	MPI_Send(NULL,0,MPI_BYTE,oc->recvone,tag,world);
#endif
      }
      break;
    }
  }

  outports[iport].status = CLOSED_PORT;
}

/* ----------------------------------------------------------------------
   infinite loop on incoming datums
   blocking MPI_Recv() for a datum
   check datum for DONE message, else callback to datumfunc()
------------------------------------------------------------------------- */

void phish_loop()
{
  int iport,doneflag;
  MPI_Status status;

  if (!checkflag) phish_error("Phish_check has not been called");

  while (1) {
    MPI_Recv(rbuf,MAXBUF,MPI_BYTE,MPI_ANY_SOURCE,MPI_ANY_TAG,world,&status);

    iport = status.MPI_TAG;
    if (iport >= MAXPORT) {
      iport -= MAXPORT;
      doneflag = 1;
    } else doneflag = 0;

    InputPort *ip = &inports[iport];
    if (ip->status != OPEN_PORT)
      phish_error("Received datum on closed or unused port");

    if (doneflag) {
      ip->donecount++;
      if (ip->donecount == ip->donemax) {
	ip->status = CLOSED_PORT;
	if (ip->donefunc) (*ip->donefunc)();
	donecount++;
	if (donecount == ninports && alldonefunc) (*alldonefunc)();
	return;
      }

    } else {
      rcount++;
      if (ip->datumfunc) {
	MPI_Get_count(&status,MPI_BYTE,&nrbytes);
	nrfields = *(int *) rbuf;
	rptr = rbuf + sizeof(int);
	nunpack = 0;
	(*ip->datumfunc)(nrfields);
      }
    }
  }
}

/* ----------------------------------------------------------------------
   infinite loop on incoming datums
   non-blocking MPI_Iprobe() for a datum
   if no datum, return to caller via probefunc() so app can do work
   if datum, check for DONE message, else callback to datumfunc()
------------------------------------------------------------------------- */

void phish_probe(void (*probefunc)())
{
  int flag,iport,doneflag;
  MPI_Status status;

  if (!checkflag) phish_error("Phish_check has not been called");
  if (!probefunc) phish_error("Phish_probe callback cannot be NULL");

  while (1) {
    MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,world,&flag,&status);
    if (flag) {
      MPI_Recv(rbuf,MAXBUF,MPI_BYTE,MPI_ANY_SOURCE,MPI_ANY_TAG,world,&status);

      iport = status.MPI_TAG;
      if (iport >= MAXPORT) {
	iport -= MAXPORT;
	doneflag = 1;
      } else doneflag = 0;

      InputPort *ip = &inports[iport];
      if (ip->status != OPEN_PORT)
	phish_error("Received datum on closed or unused port");

      if (doneflag) {
	ip->donecount++;
	if (ip->donecount == ip->donemax) {
	  ip->status = CLOSED_PORT;
	  if (ip->donefunc) (*ip->donefunc)();
	  donecount++;
	  if (donecount == ninports && alldonefunc) (*alldonefunc)();
	  return;
	}

      } else {
	rcount++;
	if (ip->datumfunc) {
	  MPI_Get_count(&status,MPI_BYTE,&nrbytes);
	  nrfields = *(int *) rbuf;
	  rptr = rbuf + sizeof(int);
	  nunpack = 0;
	  (*ip->datumfunc)(nrfields);
	}
      }
    } else (*probefunc)();
  }
}

/* ----------------------------------------------------------------------
   check for a message and recv it if there is one
   no datum callback is invoked, even if one is defined
   this allows app to request datums explicitly
     as alternative to phish_loop() and the callback it invokes
   return 0 if no message
   return -1 for done message, after invoking done callbacks
   return N for # fields in datum
------------------------------------------------------------------------- */

int phish_recv()
{
  if (!checkflag) phish_error("Phish_check has not been called");

  int flag;
  MPI_Status status;

  MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,world,&flag,&status);
  if (!flag) return 0;

  MPI_Recv(rbuf,MAXBUF,MPI_BYTE,MPI_ANY_SOURCE,MPI_ANY_TAG,world,&status);

  int doneflag;
  int iport = status.MPI_TAG;
  if (iport >= MAXPORT) {
    iport -= MAXPORT;
    doneflag = 1;
  } else doneflag = 0;
    
  InputPort *ip = &inports[iport];
  if (ip->status != OPEN_PORT)
    phish_error("Received datum on closed or unused port");
    
  if (doneflag) {
    ip->donecount++;
    if (ip->donecount == ip->donemax) {
      ip->status = CLOSED_PORT;
      if (ip->donefunc) (*ip->donefunc)();
      donecount++;
      if (donecount == ninports && alldonefunc) (*alldonefunc)();
    }
    return -1;
  }

  rcount++;
  MPI_Get_count(&status,MPI_BYTE,&nrbytes);
  nrfields = *(int *) rbuf;
  rptr = rbuf + sizeof(int);
  nunpack = 0;
  return nrfields;
}

/* ----------------------------------------------------------------------
   send datum packed in sbuf via output port iport to a downstream proc
------------------------------------------------------------------------- */

void phish_send(int iport)
{
  if (iport < 0 || iport >= MAXPORT) 
    phish_error("Invalid port ID for phish_send");
  OutPort *op = &outports[iport];
  if (op->status == UNUSED_PORT) return;
  if (op->status == CLOSED_PORT) 
    phish_error("Using phish_send with closed port");

  scount++;

  // setup send buffer

  *(int *) sbuf = npack;
  nsbytes = sptr - sbuf;

  // loop over connections
  // send datum to connection receiver via send()

  for (int iconnect = 0; iconnect < op->nconnect; iconnect++)
    send(&op->connects[iconnect]);

  // reset send buffer

  sptr = sbuf + sizeof(int);
  npack = 0;
}

/* ----------------------------------------------------------------------
   send datum packed in sbuf via output port iport to a downstream proc
   choose proc based on hash of key of length keybytes
------------------------------------------------------------------------- */

void phish_send_key(int iport, char *key, int keybytes)
{
  if (iport < 0 || iport >= MAXPORT)
    phish_error("Invalid port ID for phish_send_key");
  OutPort *op = &outports[iport];
  if (op->status == UNUSED_PORT) return;
  if (op->status == CLOSED_PORT) 
    phish_error("Using phish_send_key with closed port");

  scount++;

  // setup send buffer

  *(int *) sbuf = npack;
  nsbytes = sptr - sbuf;

  // loop over connections
  // send datum to connection receiver via send()
  // handle HASHED style here via hashing key into processor offset
  // pass non-HASHED styles to send()

  for (int iconnect = 0; iconnect < op->nconnect; iconnect++) {
    OutConnect *oc = &op->connects[iconnect];

    switch (oc->style) {
    case HASHED:
      {
	int tag = oc->recvport;
	int offset = hashlittle(key,keybytes,oc->nrecv) % oc->nrecv;
#ifdef PHISH_SAFE_SEND
	MPI_Ssend(sbuf,nsbytes,MPI_BYTE,oc->recvfirst+offset,tag,world);
#else
	MPI_Send(sbuf,nsbytes,MPI_BYTE,oc->recvfirst+offset,tag,world);
#endif
      }
      break;
      
    default:
      send(oc);
    }
  }

  // reset send buffer

  sptr = sbuf + sizeof(int);
  npack = 0;
}

/* ----------------------------------------------------------------------
   send datum packed in sbuf to a downstream proc
------------------------------------------------------------------------- */

void send(OutConnect *oc)
{
  int tag = oc->recvport;

  // send datum to appropriate receiving proc depending on connection style

  switch (oc->style) {

  case SINGLE:
  case PAIRED:
  case RING:
#ifdef PHISH_SAFE_SEND
    MPI_Ssend(sbuf,nsbytes,MPI_BYTE,oc->recvone,tag,world);
#else
    MPI_Send(sbuf,nsbytes,MPI_BYTE,oc->recvone,tag,world);
#endif
    break;

  case ROUNDROBIN:
#ifdef PHISH_SAFE_SEND
    MPI_Ssend(sbuf,nsbytes,MPI_BYTE,oc->recvfirst+oc->offset,tag,world);
#else
    MPI_Send(sbuf,nsbytes,MPI_BYTE,oc->recvfirst+oc->offset,tag,world);
#endif
    oc->offset++;
    if (oc->offset == oc->nrecv) oc->offset = 0;
    break;

  case CHAIN:
    if (oc->nrecv) {
#ifdef PHISH_SAFE_SEND
      MPI_Ssend(sbuf,nsbytes,MPI_BYTE,oc->recvone,tag,world);
#else
      MPI_Send(sbuf,nsbytes,MPI_BYTE,oc->recvone,tag,world);
#endif
    }
    break;

  case HASHED:
    phish_error("Cannot use phish_send() when hashing is required");
    break;
  }
}

/* ----------------------------------------------------------------------
   pack the sbuf with values
   first field = value type
   second field (only for arrays) = # of values
   third field = value(s)
------------------------------------------------------------------------- */

void phish_pack_datum(char *buf, int len)
{
  if (len > MAXBUF) phish_error("Send buffer overflow");

  memcpy(sbuf,buf,len);
  sptr = sbuf + len;
  npack = *(int *) sbuf;
}

void phish_pack_raw(char *buf, int len)
{
  if (sptr + 2*sizeof(int) + len - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_RAW;
  sptr += sizeof(int);
  *(int *) sptr = len;
  sptr += sizeof(int);
  memcpy(sptr,buf,len);
  sptr += len;
  npack++;
}

void phish_pack_byte(char value)
{
  if (sptr + sizeof(int) + sizeof(char) - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_BYTE;
  sptr += sizeof(int);
  *sptr = value;
  sptr += sizeof(char);
  npack++;
}

void phish_pack_int(int value)
{
  if (sptr + 2*sizeof(int) - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_INT;
  sptr += sizeof(int);
  *(int *) sptr = value;
  sptr += sizeof(int);
  npack++;
}

void phish_pack_uint64(uint64_t value)
{
  if (sptr + sizeof(int) + sizeof(uint64_t) - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_UINT64;
  sptr += sizeof(int);
  *(uint64_t *) sptr = value;
  sptr += sizeof(uint64_t);
  npack++;
}

void phish_pack_double(double value)
{
  if (sptr + sizeof(int) + sizeof(double) - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_DOUBLE;
  sptr += sizeof(int);
  *(double *) sptr = value;
  sptr += sizeof(double);
  npack++;
}

void phish_pack_string(char *str)
{
  int nbytes = strlen(str) + 1;
  if (sptr + 2*sizeof(int) + nbytes - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_STRING;
  sptr += sizeof(int);
  *(int *) sptr = nbytes;
  sptr += sizeof(int);
  memcpy(sptr,str,nbytes);
  sptr += nbytes;
  npack++;
}

void phish_pack_int_array(int *vec, int n)
{
  int nbytes = n*sizeof(int);
  if (sptr + 2*sizeof(int) + nbytes - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_INT_ARRAY;
  sptr += sizeof(int);
  *(int *) sptr = n;
  sptr += sizeof(int);
  memcpy(sptr,vec,nbytes);
  sptr += nbytes;
  npack++;
}

void phish_pack_uint64_array(uint64_t *vec, int n)
{
  int nbytes = n*sizeof(uint64_t);
  if (sptr + 2*sizeof(int) + nbytes - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_UINT64_ARRAY;
  sptr += sizeof(int);
  *(int *) sptr = n;
  sptr += sizeof(int);
  memcpy(sptr,vec,nbytes);
  sptr += nbytes;
  npack++;
}

void phish_pack_double_array(double *vec, int n)
{
  int nbytes = n*sizeof(uint64_t);
  if (sptr + 2*sizeof(int) + nbytes - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_DOUBLE_ARRAY;
  sptr += sizeof(int);
  *(int *) sptr = n;
  sptr += sizeof(int);
  memcpy(sptr,vec,nbytes);
  sptr += nbytes;
  npack++;
}

/* ----------------------------------------------------------------------
   process field rbuf, one field at a time
   return value = field type
   buf = ptr to field
   len = byte count for RAW and STRING (including NULL)
   len = 1 for BYTE, INT, UINT64, DOUBLE
   len = # of array values for ARRAY types
------------------------------------------------------------------------- */

int phish_unpack(char **buf, int *len)
{
  if (nunpack == nrfields) phish_error("Recv buffer empty");

  int type = *(int *) rptr;
  rptr += sizeof(int);

  int nbytes;
  switch (type) {
  case PHISH_RAW:
    *len = nbytes = *(int *) rptr;
    rptr += sizeof(int);
    break;
  case PHISH_BYTE:
    *len = 1;
    nbytes = sizeof(char);
    break;
  case PHISH_INT:
    *len = 1;
    nbytes = sizeof(int);
    break;
  case PHISH_UINT64:
    *len = 1;
    nbytes = sizeof(uint64_t);
    break;
  case PHISH_DOUBLE:
    *len = 1;
    nbytes = sizeof(double);
    break;
  case PHISH_STRING:
    *len = nbytes = *(int *) rptr;
    rptr += sizeof(int);
    break;
  case PHISH_INT_ARRAY:
    *len = *(int *) rptr;
    rptr += sizeof(int);
    nbytes = *len * sizeof(int);
    break;
  case PHISH_UINT64_ARRAY:
    *len = *(int *) rptr;
    rptr += sizeof(int);
    nbytes = *len * sizeof(uint64_t);
    break;
  case PHISH_DOUBLE_ARRAY:
    *len = *(int *) rptr;
    rptr += sizeof(int);
    nbytes = *len * sizeof(double);
    break;
  }

  *buf = rptr;
  rptr += nbytes;

  return type;
}

/* ----------------------------------------------------------------------
   return the entire received datum
   buf = ptr to datum
   len = total size of received datum
------------------------------------------------------------------------- */

void phish_datum(char **buf, int *len)
{
  *buf = rbuf;
  *len = nrbytes;
}

/* ---------------------------------------------------------------------- */

void phish_error(const char *str)
{
  printf("ERROR: App %s ID %s # %d: %s\n",appname,appid,me-appprev,str);
  MPI_Abort(world,1);
}

/* ---------------------------------------------------------------------- */

void phish_warn(const char *str)
{
  printf("WARNING: App %s ID %s # %d: %s\n",appname,appid,me-appprev,str);
}

/* ---------------------------------------------------------------------- */

double phish_timer()
{
  return MPI_Wtime();
}

/* ---------------------------------------------------------------------- */

void stats()
{
  printf("Stats: App %s ID %s # %d: %lu %lu datums recv/sent\n",
	 appname,appid,me-appprev,rcount,scount);
}
