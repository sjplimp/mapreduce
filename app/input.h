/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#ifndef APP_INPUT_H
#define APP_INPUT_H

#include "stdio.h"
#include "pointers.h"

namespace APP_NS {

class Input : protected Pointers {
 public:
  int narg;                    // # of command args
  char **arg;                  // parsed args for command
  class Variable *variable;    // defined variables

  Input(class APP *, int, char **);
  ~Input();
  void file();                   // process all input
  void file(const char *);       // process an input script
  char *one(const char *);       // process a single command
  void substitute(char *, int);  // substitute for variables in a string

 private:
  int me;                      // proc ID
  char *command;               // ptr to current command
  int maxarg;                  // max # of args in arg
  char *line,*copy,*work;      // input line & copy of it
  int echo_screen;             // 0 = no, 1 = yes
  int echo_log;                // 0 = no, 1 = yes
  int nfile,maxfile;           // current # and max # of open input files
  int label_active;            // 0 = no label, 1 = looking for label
  char *labelstr;              // label string being looked for
  int jump_skip;               // 1 if skipping next jump, 0 otherwise

  FILE **infiles;              // list of open input files

  void parse();                // parse an input text line
  void parse_object(int);      // parse an input text line in object syntax
  int execute_command();       // execute a single command

  void clear();                // input script commands
  void echo();
  void ifthenelse();
  void include();
  void jump();
  void label();
  void log();
  void next_command();
  void print();
  void variable_command();
};

}

#endif
