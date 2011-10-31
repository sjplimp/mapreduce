// PHISH library

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "stdint.h"
#include "phish.h"
#include "hash.h"

//#define PHISH_SSEND 1      // uncomment for safer/slower MPI_Ssend()

typedef void (DoneFunc)();
typedef void (DatumFunc)(int);
typedef void (ProbeFunc)();

MPI_Comm world;
int me,nprocs;

char *spname;
DatumFunc *datumfunc = NULL;
DoneFunc *donefunc = NULL;
ProbeFunc *probefunc = NULL;

struct Recv {
  int activated;
  int style;
  int sendprocs,sendfirst,sendport;
  int recvprocs,recvfirst,recvport;
};
struct Send {
  int activated;
  int style;
  int sendprocs,sendfirst,sendport;
  int recvprocs,recvfirst,recvport;
  int offset;
};
struct Recv *precv;
struct Send *psend;
int nrecv,nsend;

int sendwhich = 0;

int donelimit;
int donecount = 0;

enum{ONE2ONE,ONE2MANY,ONE2MANY_RR,MANY2ONE,MANY2MANY,MANY2MANY_ONE};

#define MAXBUF 1024*1024

char rbuf[MAXBUF];
int nrbuf;
char *rptr;

char sbuf[MAXBUF];
int nsbuf;
char *sptr;

#define DONETAG 100

/* ---------------------------------------------------------------------- */

void phish_init(const char *name, int nrecvmax, int nsendmax, 
		int *nargptr, char ***argsptr)
{
  int foo = 0;
  MPI_Init(&foo,NULL);
  //printf("PREINIT %d %s %s %s\n",*nargptr,
  //	 (*argsptr)[0],(*argsptr)[1],(*argsptr)[2]);
  //MPI_Init(nargptr,argsptr);

  world = MPI_COMM_WORLD;
  MPI_Comm_rank(world,&me);
  MPI_Comm_size(world,&nprocs);

  int n = strlen(name) + 1;
  spname = new char[n];
  strcpy(spname,name);

  nrecv = nrecvmax;
  nsend = nsendmax;
  precv = new Recv[nrecv];
  psend = new Send[nsend];
  for (int i = 0; i < nrecv; i++) precv[i].activated = 0;
  for (int i = 0; i < nsend; i++) psend[i].activated = 0;

  char **args = *argsptr;
  int narg = *nargptr;
  int argstart = narg;

  int iarg = 1;
  while (iarg < narg) {
    if (strcmp(args[iarg],"-recv") == 0) {
      int n = atoi(args[iarg+1]);

      iarg += 2;
      for (int m = 0; m < n; m++) {
	int style;
	if (strcmp(args[iarg],"one2one") == 0) style = ONE2ONE;
	else if (strcmp(args[iarg],"one2many") == 0) style = ONE2MANY;
	else if (strcmp(args[iarg],"one2many/rr") == 0) style = ONE2MANY_RR;
	else if (strcmp(args[iarg],"many2one") == 0) style = MANY2ONE;
	else if (strcmp(args[iarg],"many2many") == 0) style = MANY2MANY;
	else if (strcmp(args[iarg],"many2many/one") == 0) style = MANY2MANY_ONE;
	else phish_error("Unrecognized recv style");

	int sendprocs = atoi(args[iarg+1]);
	int sendfirst = atoi(args[iarg+2]);
	int sendport = atoi(args[iarg+3]);
	int recvprocs = atoi(args[iarg+4]);
	int recvfirst = atoi(args[iarg+5]);
	int recvport = atoi(args[iarg+6]);

	if (recvport > nrecvmax) phish_error("Recv port exceeds nrecvmax");
	int iport = recvport - 1;

	precv[iport].activated = 1;
	precv[iport].style = style;
	precv[iport].sendprocs = sendprocs;
	precv[iport].sendfirst = sendfirst;
	precv[iport].sendport = sendport;
	precv[iport].recvprocs = recvprocs;
	precv[iport].recvfirst = recvfirst;
	precv[iport].recvport = recvport;

	if (style == ONE2ONE) donelimit++;
	else if (style == ONE2MANY) donelimit++;
	else if (style == ONE2MANY_RR) donelimit++;
	else if (style == MANY2ONE) donelimit += sendprocs;
	else if (style == MANY2MANY) donelimit += sendprocs;
	else if (style == MANY2MANY_ONE) donelimit++;

	iarg += 7;
      }

    } else if (strcmp(args[iarg],"-send") == 0) {
      int n = atoi(args[iarg+1]);

      iarg += 2;
      for (int m = 0; m < n; m++) {
	int style;
	if (strcmp(args[iarg],"one2one") == 0) style = ONE2ONE;
	else if (strcmp(args[iarg],"one2many") == 0) style = ONE2MANY;
	else if (strcmp(args[iarg],"one2many/rr") == 0) style = ONE2MANY_RR;
	else if (strcmp(args[iarg],"many2one") == 0) style = MANY2ONE;
	else if (strcmp(args[iarg],"many2many") == 0) style = MANY2MANY;
	else if (strcmp(args[iarg],"many2many/one") == 0) style = MANY2MANY_ONE;
	else phish_error("Unrecognized send style");

	int sendprocs = atoi(args[iarg+1]);
	int sendfirst = atoi(args[iarg+2]);
	int sendport = atoi(args[iarg+3]);
	int recvprocs = atoi(args[iarg+4]);
	int recvfirst = atoi(args[iarg+5]);
	int recvport = atoi(args[iarg+6]);

	if (sendport > nsendmax) phish_error("Send port exceeds nsendmax");
	int iport = sendport - 1;

	psend[iport].activated = 1;
	psend[iport].style = style;
	psend[iport].sendprocs = sendprocs;
	psend[iport].sendfirst = sendfirst;
	psend[iport].sendport = sendport;
	psend[iport].recvprocs = recvprocs;
	psend[iport].recvfirst = recvfirst;
	psend[iport].recvport = recvport;
	psend[iport].offset = 0;

	iarg += 7;
      }

    } else if (strcmp(args[iarg],"-args") == 0) {
      argstart = iarg+1;
      iarg = narg;
    }
  }

  *nargptr = narg-argstart;
  if (*nargptr > 0) *argsptr = &args[argstart];
  else *argsptr = NULL;

  sptr = sbuf + sizeof(int);
  nsbuf = 0;
}

/* ---------------------------------------------------------------------- */

int phish_init_python(char *name, int nrecvmax, int nsendmax,
		      int narg, char **args)
{
  int narg_start = narg;
  phish_init(name,nrecvmax,nsendmax,&narg,&args);
  return narg_start-narg;
}

/* ---------------------------------------------------------------------- */

void phish_close()
{
  delete [] spname;
  delete [] precv;
  delete [] psend;
  MPI_Finalize();
}

/* ---------------------------------------------------------------------- */

int phish_world(int *pme, int *pnprocs)
{
  *pme = me;
  *pnprocs = nprocs;
  return world;
}

/* ---------------------------------------------------------------------- */

void phish_error(const char *str)
{
  printf("ERROR: %s\n",str);
  MPI_Abort(world,1);
}

/* ---------------------------------------------------------------------- */

double phish_timer()
{
  return MPI_Wtime();
}

/* ----------------------------------------------------------------------
   set callback functions
------------------------------------------------------------------------- */

void phish_callback_done(void (*callback)())
{
  donefunc = callback;
}

void phish_callback_datum(void (*callback)(int))
{
  datumfunc = callback;
}

void phish_callback_probe(void (*callback)())
{
  probefunc = callback;
}

/* ----------------------------------------------------------------------
   infinite loop on incoming messages
   blocking MPI_Recv() for a message
   check for DONE messages, when N = recvprocs are received, call donefunc()
   else call datumfunc() with each message
------------------------------------------------------------------------- */

void phish_loop()
{
  MPI_Status status;

  while (1) {
    MPI_Recv(rbuf,MAXBUF,MPI_BYTE,MPI_ANY_SOURCE,MPI_ANY_TAG,world,&status);
    if (status.MPI_TAG == DONETAG) {
      donecount++;
      if (donecount < donelimit) continue;
      if (donefunc) (*donefunc)();
      return;
    }
    MPI_Get_count(&status,MPI_BYTE,&nrbuf);
    rptr = rbuf + sizeof(int);
    if (datumfunc) (*datumfunc)(*(int *) rbuf);
  }
}

/* ----------------------------------------------------------------------
   infinite loop on incoming messages
   non-blocking MPI_Iprobe() for a message
   check for DONE messages, when N = recvprocs are received, call donefunc()
   else call datumfunc() with each message
   if no message, return to caller via probefunc() so app can do work
------------------------------------------------------------------------- */

void phish_probe()
{
  int flag;
  MPI_Status status;

  while (1) {
    MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,world,&flag,&status);
    if (flag) {
      MPI_Recv(rbuf,MAXBUF,MPI_BYTE,MPI_ANY_SOURCE,MPI_ANY_TAG,world,&status);
      if (status.MPI_TAG == DONETAG) {
	donecount++;
	if (donecount < donelimit) continue;
	if (donefunc) (*donefunc)();
	return;
      }
      MPI_Get_count(&status,MPI_BYTE,&nrbuf);
      rptr = rbuf + sizeof(int);
      if (datumfunc) (*datumfunc)(*(int *) rbuf);
    }
    if (probefunc) (*probefunc)();
  }
}

/* ----------------------------------------------------------------------
   process the rbuf, one value at a time
   return value type
   also return ptr to value and its len to caller
------------------------------------------------------------------------- */

int phish_unpack_next(char **buf, int *len)
{
  if (rptr - rbuf == nrbuf) phish_error("Recv buffer empty");

  int type = *(int *) rptr;
  rptr += sizeof(int);
  *buf = rptr;

  if (type == PHISH_BYTE) *len = 1;
  else if (type == PHISH_INT) *len = sizeof(int);
  else if (type == PHISH_UINT64) *len = sizeof(uint64_t);
  else if (type == PHISH_DOUBLE) *len = sizeof(double);
  else if (type == PHISH_STRING) {
    *len = strlen(rptr);
    rptr++;  // this is a kludge for trailing NULL byte
  }

  rptr += *len;

  return type;
}

void phish_unpack_raw(char **buf, int *len)
{
  *buf = rbuf;
  *len = nrbuf;
}

/* ----------------------------------------------------------------------
   pack the sbuf with values
   first field = value type
   second field (only for arrays) = # of values
   third field = value(s)
------------------------------------------------------------------------- */

void phish_pack_byte(char value)
{
  if (sptr + sizeof(int) + 1 - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_BYTE;
  sptr += sizeof(int);
  *sptr = value;
  sptr++;
  nsbuf++;
}

void phish_pack_int(int value)
{
  if (sptr + 2*sizeof(int) - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_INT;
  sptr += sizeof(int);
  *(int *) sptr = value;
  sptr += sizeof(int);
  nsbuf++;
}

void phish_pack_uint64(uint64_t value)
{
  if (sptr + sizeof(int) + sizeof(uint64_t) - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_UINT64;
  sptr += sizeof(int);
  *(uint64_t *) sptr = value;
  sptr += sizeof(uint64_t);
  nsbuf++;
}

void phish_pack_double(double value)
{
  if (sptr + sizeof(int) + sizeof(double) - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_DOUBLE;
  sptr += sizeof(int);
  *(double *) sptr = value;
  sptr += sizeof(double);
  nsbuf++;
}

void phish_pack_string(char *str)
{
  int n = strlen(str);
  if (sptr + sizeof(int) + n+1 - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  *(int *) sptr = PHISH_STRING;
  sptr += sizeof(int);
  memcpy(sptr,str,n+1);
  sptr += n+1;
  nsbuf++;
}

void phish_pack_raw(int nvalues, char *buf, int len)
{
  if (sptr + sizeof(int) + len - sbuf > MAXBUF)
    phish_error("Send buffer overflow");

  memcpy(sbuf,buf,len);
  sptr = sbuf + len;
  nsbuf = nvalues;
}

/* ----------------------------------------------------------------------
   send sbuf to a downstream processor
------------------------------------------------------------------------- */

void phish_send()
{
  if (sendwhich < 0 || sendwhich > nsend-1 || !psend[sendwhich].activated)
    phish_error("Invalid send protocol in phish_send");

  struct Send *s = &psend[sendwhich];
  *(int *) sbuf = nsbuf;
  int nbuf = sptr - sbuf;

  switch (s->style) {
  case ONE2ONE:
  case MANY2ONE:
#ifdef PHISH_SSEND
    MPI_Ssend(sbuf,nbuf,MPI_BYTE,s->recvfirst,0,world);
#else
    MPI_Send(sbuf,nbuf,MPI_BYTE,s->recvfirst,0,world);
#endif
    break;
  case ONE2MANY_RR:
#ifdef PHISH_SSEND
    MPI_Ssend(sbuf,nbuf,MPI_BYTE,s->recvfirst + s->offset,0,world);
#else
    MPI_Send(sbuf,nbuf,MPI_BYTE,s->recvfirst + s->offset,0,world);
#endif
    s->offset++;
    if (s->offset == s->recvprocs) s->offset = 0;
    break;
  case ONE2MANY:
  case MANY2MANY:
    phish_error("Cannot phish_send() when hashing is required");
    break;
  case MANY2MANY_ONE:
#ifdef PHISH_SSEND
    MPI_Ssend(sbuf,nbuf,MPI_BYTE,s->recvfirst + (me - s->sendfirst),0,world);
#else
    MPI_Send(sbuf,nbuf,MPI_BYTE,s->recvfirst + (me - s->sendfirst),0,world);
#endif
    break;
  }

  sptr = sbuf + sizeof(int);
  nsbuf = 0;
}

/* ----------------------------------------------------------------------
   send buf of nbytes to a downstream processor
   use send protocol iwhichsend
---------------------------------------------------------------- */

void phish_send_type(int iwhich)
{
  sendwhich = iwhich-1;
  phish_send();
  sendwhich = 0;
}

/* ----------------------------------------------------------------------
   send sbuf to a downstream processor based on hash of key
------------------------------------------------------------------------- */

void phish_send_key(char *key, int keybytes)
{
  if (sendwhich < 0 || sendwhich > nsend-1 || !psend[sendwhich].activated) 
    phish_error("Invalid send protocol in phish_send_key");

  struct Send *s = &psend[sendwhich];
  *(int *) sbuf = nsbuf;
  int nbuf = sptr - sbuf;

  switch (s->style) {
  case ONE2MANY:
  case MANY2MANY:
    {
      int index = hashlittle(key,keybytes,s->recvprocs) % s->recvprocs;
#ifdef PHISH_SSEND
      MPI_Ssend(sbuf,nbuf,MPI_BYTE,s->recvfirst + index,0,world);
#else
      MPI_Send(sbuf,nbuf,MPI_BYTE,s->recvfirst + index,0,world);
#endif
    }
    break;
  default:
    phish_send();
  }

  sptr = sbuf + sizeof(int);
  nsbuf = 0;
}

/* ----------------------------------------------------------------------
   send buf of nbytes to a downstream processor based on hash of key
   use send protocol iwhich
------------------------------------------------------------------------- */

void phish_send_key_type(char *key, int keybytes, int iwhich)
{
  sendwhich = iwhich-1;
  phish_send_key(key,keybytes);
  sendwhich = 0;
}

/* ----------------------------------------------------------------------
   send DONE message to all processors downstream of me
------------------------------------------------------------------------- */

void phish_send_done()
{
  for (int isend = 0; isend < nsend; isend++) {
    struct Send *s = &psend[isend];
    if (!s->activated) continue;
    switch (s->style) {
    case ONE2ONE:
    case MANY2ONE:
#ifdef PHISH_SSEND
      MPI_Ssend(NULL,0,MPI_BYTE,s->recvfirst,DONETAG,world);
#else
      MPI_Send(NULL,0,MPI_BYTE,s->recvfirst,DONETAG,world);
#endif
      break;
    case ONE2MANY:
    case ONE2MANY_RR:
    case MANY2MANY:
      for (int i = 0; i < s->recvprocs; i++)
#ifdef PHISH_SSEND
        MPI_Ssend(NULL,0,MPI_BYTE,s->recvfirst + i,DONETAG,world);
#else
        MPI_Send(NULL,0,MPI_BYTE,s->recvfirst + i,DONETAG,world);
#endif
      break;
    case MANY2MANY_ONE:
#ifdef PHISH_SSEND
      MPI_Ssend(NULL,0,MPI_BYTE,s->recvfirst + (me - s->sendfirst),
		DONETAG,world);
#else
      MPI_Send(NULL,0,MPI_BYTE,s->recvfirst + (me - s->sendfirst),
	       DONETAG,world);
#endif
      break;
    }
  }
}
