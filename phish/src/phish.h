/* PHISH library */

#ifndef PHISH_H
#define PHISH_H

#include "stdint.h"

#define PHISH_BYTE 0
#define PHISH_INT 1
#define PHISH_UINT64 2
#define PHISH_DOUBLE 3
#define PHISH_STRING 4
#define PHISH_BYTEARRAY 5
#define PHISH_INTARRAY 6
#define PHISH_UINT64ARRAY 7
#define PHISH_DOUBLEARRAY 8

#ifdef __cplusplus
extern "C" {
#endif

void phish_init(const char *, int, int, int *, char ***);
int phish_init_python(char *, int, int, int, char **);
void phish_close();
int phish_world(int *, int *);

void phish_error(const char *);
double phish_timer();

void phish_callback_done(void (*)());
void phish_callback_datum(void (*)(int));
void phish_callback_probe(void (*)());

void phish_loop();
void phish_probe();

int phish_unpack_next(char **, int *);
void phish_unpack_raw(char **, int *);

void phish_pack_byte(char);
void phish_pack_int(int);
void phish_pack_uint64(uint64_t);
void phish_pack_double(double);
void phish_pack_string(char *);
void phish_pack_raw(int, char *, int);

void phish_send();
void phish_send_type(int);
void phish_send_key(char *, int);
void phish_send_key_type(char *, int, int);
void phish_send_done();

#ifdef __cplusplus
}
#endif

#endif
