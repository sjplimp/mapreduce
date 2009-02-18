#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Convert MatrixMarket ASCII file to Karl's format binary file.  */
/* Useful for debugging the code that reads Karl's files.         */
/* Convert a matrix market file to Karl's format, run link2graph, */
/* and compare the link2graph output with the matrix market file. */
/* To compile:  cc MM2Karl.c                                      */
/* To run:  a.out input_MM_file output_Karl_file                  */

#define skipline(ch, fp) while ((ch = getc(fpin)) != '\n');

main(int narg, char *arg[])
{
char ch;
char buffer[32];
FILE *fpin, *fpout;
int N, M, nnz, i, j;

  memset(buffer, 0, 32);
  fpin = fopen(arg[1], "rw");
  fpout = fopen(arg[2], "w");

  while ((ch = getc(fpin)) == '%') skipline(ch, fpin);
  /* skip N */
  while ((ch = getc(fpin)) != ' ');
  fscanf(fpin, "%d %d", &M, &nnz);
  printf("M %d NNZ %d\n", M, nnz);
  int *bufa = (int *) &buffer[0];
  int *bufb = (int *) &buffer[8];
  int *bufc = (int *) &buffer[16];
  int *bufd = (int *) &buffer[24];
  for (i = 0; i < nnz; i++) {
    int ii, jj;
    float junk;
    fscanf(fpin, "%d %d %f", &ii, &jj, &junk);
    *bufa = ii;
    *bufb = ii;
    *bufc = jj;
    *bufd = jj;
    printf("%d %d:  %d %d %d %d\n", ii, jj, *bufa, *bufb, *bufc, *bufd);
    printf("Writing ... ");
    for (j = 0; j < 32; j++) printf("%c", buffer[j]);
    printf("\n");
    fwrite(buffer, 1, 32, fpout);
  }
  fclose(fpin);
  fclose(fpout);
}
