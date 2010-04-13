/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "ctype.h"
#include "input.h"
#include "style_command.h"
#include "universe.h"
#include "object.h"
#include "variable.h"
#include "error.h"
#include "memory.h"

using namespace APP_NS;

#define MAXLINE 2048
#define DELTA 4

/* ---------------------------------------------------------------------- */

Input::Input(APP *app, int argc, char **argv) : Pointers(app)
{
  MPI_Comm_rank(world,&me);

  line = new char[MAXLINE];
  copy = new char[MAXLINE];
  work = new char[MAXLINE];
  narg = maxarg = 0;
  arg = NULL;

  echo_screen = 0;
  echo_log = 1;

  label_active = 0;
  labelstr = NULL;
  jump_skip = 0;

  if (me == 0) {
    nfile = maxfile = 1;
    infiles = (FILE **) memory->smalloc(sizeof(FILE *),"input:infiles");
    infiles[0] = infile;
  } else infiles = NULL;

  variable = new Variable(app);

  // process command-line args
  // check for args "-var" and "-echo"
  // caller has already checked that sufficient arguments exist

  int iarg = 0;
  while (iarg < argc) {
    if (strcmp(argv[iarg],"-var") == 0) {
      variable->set(argv[iarg+1],argv[iarg+2]);
      iarg += 3;
    } else if (strcmp(argv[iarg],"-echo") == 0) {
      narg = 1;
      char **tmp = arg;        // trick echo() into using argv instead of arg
      arg = &argv[iarg+1];
      echo();
      arg = tmp;
      iarg += 2;
    } else iarg++;
  }
}

/* ---------------------------------------------------------------------- */

Input::~Input()
{
  // don't free command and arg strings
  // they just point to other allocated memory

  delete variable;
  delete [] line;
  delete [] copy;
  delete [] work;
  if (labelstr) delete [] labelstr;
  if (arg) memory->sfree(arg);
  if (infiles) memory->sfree(infiles);
}

/* ----------------------------------------------------------------------
   process all input from infile
   infile = stdin or file if command-line arg "-in" was used
------------------------------------------------------------------------- */

void Input::file()
{
  int m,n;

  while (1) {
    
    // read a line from input script
    // if line ends in continuation char '&', concatenate next line(s)
    // n = length of line including str terminator, 0 if end of file
    // m = position of last printable char in line or -1 if blank line

    if (me == 0) {
      m = 0;
      while (1) {
	if (fgets(&line[m],MAXLINE-m,infile) == NULL) n = 0;
	else n = strlen(line) + 1;
	if (n == 0) break;
	m = n-2;
	while (m >= 0 && isspace(line[m])) m--;
	if (m < 0 || line[m] != '&') break;
      }
    }

    // bcast the line
    // if n = 0, end-of-file
    // error if label_active is set, since label wasn't encountered
    // if original input file, code is done
    // else go back to previous input file

    MPI_Bcast(&n,1,MPI_INT,0,world);
    if (n == 0) {
      if (label_active) error->all("Label wasn't found in input script");
      if (me == 0) {
	if (infile != stdin) fclose(infile);
	nfile--;
      }
      MPI_Bcast(&nfile,1,MPI_INT,0,world);
      if (nfile == 0) break;
      if (me == 0) infile = infiles[nfile-1];
      continue;
    }

    MPI_Bcast(line,n,MPI_CHAR,0,world);

    // if n = MAXLINE, line is too long

    if (n == MAXLINE) {
      char str[MAXLINE+32];
      sprintf(str,"Input line too long: %s",line);
      error->all(str);
    }

    // echo the command unless scanning for label

    if (me == 0 && label_active == 0) {
      if (echo_screen && screen) fprintf(screen,"%s",line); 
      if (echo_log && logfile) fprintf(logfile,"%s",line);
    }

    // parse the line
    // if no command, skip to next line in input script

    parse();
    if (command == NULL) continue;

    // if scanning for label, skip command unless it's a label command

    if (label_active && strcmp(command,"label") != 0) continue;

    // execute the command

    if (execute_command()) {
      char str[MAXLINE];
      sprintf(str,"Unknown command: %s",line);
      error->all(str);
    }
  }
}

/* ----------------------------------------------------------------------
   process all input from filename
------------------------------------------------------------------------- */

void Input::file(const char *filename)
{
  // error if another nested file still open
  // if single open file is not stdin, close it
  // open new filename and set infile, infiles[0]

  if (me == 0) {
    if (nfile > 1)
      error->one("Another input script is already being processed");
    if (infile != stdin) fclose(infile);
    infile = fopen(filename,"r");
    if (infile == NULL) {
      char str[128];
      sprintf(str,"Cannot open input script %s",filename);
      error->one(str);
    }
    infiles[0] = infile;
  } else infile = NULL;

  file();
}

/* ----------------------------------------------------------------------
   parse the command in single and execute it
   return command name to caller
------------------------------------------------------------------------- */

char *Input::one(const char *single)
{
  strcpy(line,single);

  // echo the command unless scanning for label
  
  if (me == 0 && label_active == 0) {
    if (echo_screen && screen) fprintf(screen,"%s",line); 
    if (echo_log && logfile) fprintf(logfile,"%s",line);
  }

  // parse the line
  // if no command, just return NULL

  parse();
  if (command == NULL) return NULL;

  // if scanning for label, skip command unless it's a label command

  if (label_active && strcmp(command,"label") != 0) return NULL;

  // execute the command and return its name

  if (execute_command()) {
    char str[MAXLINE];
    sprintf(str,"Unknown command: %s",line);
    error->all(str);
  }

  return command;
}

/* ----------------------------------------------------------------------
   parse copy of command line as series of whitespace separated words
   strip comment = all chars from # on
   replace all $ via variable substitution
   command = first word
   narg = # of args
   arg[] = individual args
   treat text between double quotes as one arg
------------------------------------------------------------------------- */

void Input::parse()
{
  // make a copy to work on

  strcpy(copy,line);

  // strip any # comment by resetting string terminator
  // do not strip # inside double quotes

  int level = 0;
  char *ptr = copy;
  while (*ptr) {
    if (*ptr == '#' && level == 0) {
      *ptr = '\0';
      break;
    }
    if (*ptr == '"') {
      if (level == 0) level = 1;
      else level = 0;
    }
    ptr++;
  }

  // perform $ variable substitution (print changes)
  // except if searching for a label since earlier variable may not be defined

  if (!label_active) substitute(copy,1);

  // command = 1st arg

  command = strtok(copy," \t\n\r\f");
  if (command == NULL) return;

  // point arg[] at each subsequent arg
  // treat text between double quotes as one arg
  // insert string terminators in copy to delimit args

  narg = 0;
  while (1) {
    if (narg == maxarg) {
      maxarg += DELTA;
      arg = (char **) memory->srealloc(arg,maxarg*sizeof(char *),"input:arg");
    }
    arg[narg] = strtok(NULL," \t\n\r\f");
    if (arg[narg] && arg[narg][0] == '\"') {
      arg[narg] = &arg[narg][1];
      if (arg[narg][strlen(arg[narg])-1] == '\"')
	arg[narg][strlen(arg[narg])-1] = '\0';
      else {
	arg[narg][strlen(arg[narg])] = ' ';
	ptr = strtok(arg[narg],"\"");
	if (ptr == NULL) error->all("Unbalanced quotes in input line");
      }
    }
    if (arg[narg]) narg++;
    else break;
  }

  // check if need to re-parse via object syntax

  if (strchr(command,'(')) parse_object(0);
  else if (narg >= 2 && strcmp(arg[0],"=") == 0 && strchr(arg[1],'('))
    parse_object(1);
}

/* ----------------------------------------------------------------------
   parse copy of command line via object syntax
   mode = 0 for b.c(d,e,...) -> b NULL c d e ...
   mode = 1 for a = b.c(d,e,...) -> b a c d e ...
   otherwise follow same parsing rules as parse()
------------------------------------------------------------------------- */

void Input::parse_object(int mode)
{
  // make a copy to work on

  strcpy(copy,line);

  // strip any # comment by resetting string terminator
  // do not strip # inside double quotes

  int level = 0;
  char *ptr = copy;
  while (*ptr) {
    if (*ptr == '#' && level == 0) {
      *ptr = '\0';
      break;
    }
    if (*ptr == '"') {
      if (level == 0) level = 1;
      else level = 0;
    }
    ptr++;
  }

  // perform $ variable substitution (print changes)
  // except if searching for a label since earlier variable may not be defined

  if (!label_active) substitute(copy,1);

  // for mode = 0, extract b,c
  // for mode = 1, extract a,b,c

  if (maxarg < 2) {
    maxarg += DELTA;
    arg = (char **) memory->srealloc(arg,maxarg*sizeof(char *),"input:arg");
  }

  if (mode == 0) {
    command = strtok(copy," \t\n\r\f(");
    arg[0] = NULL;
    char *period = strchr(command,'.');
    if (period) {
      *period = '\0';
      arg[1] = period+1;
    } else arg[1] = NULL;
  } else {
    arg[0] = strtok(copy," \t\n\r\f");
    char *equal = strtok(NULL," \t\n\r\f");
    command = strtok(NULL," \t\n\r\f(");
    char *period = strchr(command,'.');
    if (period) {
      *period = '\0';
      arg[1] = period+1;
    } else arg[1] = NULL;
  }

  // point arg[] at each subsequent arg following opening left paren
  // treat text between double quotes as one arg
  // insert string terminators in copy to delimit args

  narg = 2;
  while (1) {
    if (narg == maxarg) {
      maxarg += DELTA;
      arg = (char **) memory->srealloc(arg,maxarg*sizeof(char *),"input:arg");
    }
    arg[narg] = strtok(NULL," \t\n\r\f,)");
    if (arg[narg] && arg[narg][0] == '\"') {
      arg[narg] = &arg[narg][1];
      if (arg[narg][strlen(arg[narg])-1] == '\"')
	arg[narg][strlen(arg[narg])-1] = '\0';
      else {
	arg[narg][strlen(arg[narg])] = ' ';
	ptr = strtok(arg[narg],"\"");
	if (ptr == NULL) error->all("Unbalanced quotes in input line");
      }
    }
    if (arg[narg]) narg++;
    else break;
  }
}

/* ----------------------------------------------------------------------
   substitute for $ variables in str
   print updated string if flag is set and not searching for label
------------------------------------------------------------------------- */

void Input::substitute(char *str, int flag)
{
  // use work[] as scratch space to expand str
  // do not replace $ inside double quotes as flagged by level
  // var = pts at variable name, ended by NULL
  //   if $ is followed by '{', trailing '}' becomes NULL
  //   else $x becomes x followed by NULL
  // beyond = pts at text following variable

  char *var,*value,*beyond;
  int level = 0;
  char *ptr = str;

  while (*ptr) {
    if (*ptr == '$' && level == 0) {
      if (*(ptr+1) == '{') {
	var = ptr+2;
	int i = 0;
	while (var[i] != '\0' && var[i] != '}') i++;
	if (var[i] == '\0') error->one("Invalid variable name");
	var[i] = '\0';
	beyond = ptr + strlen(var) + 3;
      } else {
	var = ptr;
	var[0] = var[1];
	var[1] = '\0';
	beyond = ptr + strlen(var) + 1;
      }
      value = variable->retrieve(var);
      if (value == NULL) error->one("Substitution for illegal variable");

      *ptr = '\0';
      strcpy(work,str);
      if (strlen(work)+strlen(value) >= MAXLINE)
	error->one("Input line too long after variable substitution");
      strcat(work,value);
      if (strlen(work)+strlen(beyond) >= MAXLINE)
	error->one("Input line too long after variable substitution");
      strcat(work,beyond);
      strcpy(str,work);
      ptr += strlen(value);
      if (flag && me == 0 && label_active == 0) {
	if (echo_screen && screen) fprintf(screen,"%s",str); 
	if (echo_log && logfile) fprintf(logfile,"%s",str);
      }
      continue;
    }
    if (*ptr == '"') {
      if (level == 0) level = 1;
      else level = 0;
    }
    ptr++;
  }
}

/* ----------------------------------------------------------------------
   process a single parsed command
   return 0 if successful, -1 if did not recognize command
------------------------------------------------------------------------- */

int Input::execute_command()
{
  int flag = 1;

  if (!strcmp(command,"clear")) clear();
  else if (!strcmp(command,"echo")) echo();
  else if (!strcmp(command,"if")) ifthenelse();
  else if (!strcmp(command,"include")) include();
  else if (!strcmp(command,"jump")) jump();
  else if (!strcmp(command,"label")) label();
  else if (!strcmp(command,"log")) log();
  else if (!strcmp(command,"next")) next_command();
  else if (!strcmp(command,"print")) print();
  else if (!strcmp(command,"variable")) variable_command();

  else flag = 0;

  // return if command was listed above

  if (flag) return 0;

  // check if command is an Object
  // oflag = 1 = command is an Object class
  // oflag = 2 = command is an Object instance

  int index;
  int oflag = obj->classify_object(command,index);

  if (oflag == 1) {
    obj->create_object(index,narg,arg);
    return 0;
  } else if (oflag == 2) {
    obj->invoke_object(index,narg,arg);
    return 0;
  }

  // remove double NULL args if command is in object syntax
  // a(b,c,...) -> a NULL NULL b c ... -> a b c ...

  int offset = 0;
  if (narg >= 2 && arg[0] == NULL && arg[1] == NULL) offset = 2;

  // check if command is added via style_command.h

  if (0) return 0;      // dummy line to enable else-if macro expansion

#define COMMAND_CLASS
#define CommandStyle(key,Class)         \
  else if (strcmp(command,#key) == 0) { \
    Class key(app);			\
    key.command(narg-offset,&arg[offset]);              \
    return 0;                           \
  }
#include "style_command.h"
#undef COMMAND_CLASS

  // unrecognized command

  return -1;
}

/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */

void Input::clear()
{
  if (narg > 0) error->all("Illegal clear command");
  app->destroy();
  app->create();
}

/* ---------------------------------------------------------------------- */

void Input::echo()
{
  if (narg != 1) error->all("Illegal echo command");

  if (strcmp(arg[0],"none") == 0) {
    echo_screen = 0;
    echo_log = 0;
  } else if (strcmp(arg[0],"screen") == 0) {
    echo_screen = 1;
    echo_log = 0;
  } else if (strcmp(arg[0],"log") == 0) {
    echo_screen = 0;
    echo_log = 1;
  } else if (strcmp(arg[0],"both") == 0) {
    echo_screen = 1;
    echo_log = 1;
  } else error->all("Illegal echo command");
}

/* ---------------------------------------------------------------------- */

void Input::ifthenelse()
{
  if (narg < 5) error->all("Illegal if command");

  // flag = 0 for "then"
  // flag = 1 for "else"

  int flag = 0;
  if (strcmp(arg[1],"==") == 0) {
    if (atof(arg[0]) == atof(arg[2])) flag = 1;
  } else if (strcmp(arg[1],"!=") == 0) {
    if (atof(arg[0]) != atof(arg[2])) flag = 1;
  } else if (strcmp(arg[1],"<") == 0) {
    if (atof(arg[0]) < atof(arg[2])) flag = 1;
  } else if (strcmp(arg[1],"<=") == 0) {
    if (atof(arg[0]) <= atof(arg[2])) flag = 1;
  } else if (strcmp(arg[1],">") == 0) {
    if (atof(arg[0]) > atof(arg[2])) flag = 1;
  } else if (strcmp(arg[1],">=") == 0) {
    if (atof(arg[0]) >= atof(arg[2])) flag = 1;
  } else error->all("Illegal if command");

  // first = arg index of first "then" or "else" command
  // last = arg index of last "then" or "else" command
  
  int iarg,first,last;

  // identify range of commands within arg list for then or else
  // for else, if no comands, just return

  if (strcmp(arg[3],"then") != 0) error->all("Illegal if command");
  if (flag) {
    iarg = first = 4;
    while (iarg < narg && strcmp(arg[iarg],"else") != 0) iarg++;
    last = iarg-1;
  } else {
    iarg = 4;
    while (iarg < narg && strcmp(arg[iarg],"else") != 0) iarg++;
    if (iarg == narg) return;
    first = iarg+1;
    last = narg-1;
  }

  int ncommands = last-first + 1;
  if (ncommands <= 0) error->all("Illegal if command");

  // make copies of arg strings that are commands
  // required because re-parsing commands via one() will wipe out args

  char **commands = new char*[ncommands];
  ncommands = 0;
  for (int i = first; i <= last; i++) {
    int n = strlen(arg[i]) + 1;
    if (n == 1) error->all("Illegal if command");
    commands[ncommands] = new char[n];
    strcpy(commands[ncommands],arg[i]);
    ncommands++;
  }

  // execute the list of commands

  for (int i = 0; i < ncommands; i++)
    char *command = input->one(commands[i]);

  // clean up

  for (int i = 0; i < ncommands; i++) delete [] commands[i];
  delete [] commands;
}

/* ---------------------------------------------------------------------- */

void Input::include()
{
  if (narg != 1) error->all("Illegal include command");

  if (me == 0) {
    if (nfile == maxfile) {
      maxfile++;
      infiles = (FILE **) 
        memory->srealloc(infiles,maxfile*sizeof(FILE *),"input:infiles");
    }
    infile = fopen(arg[0],"r");
    if (infile == NULL) {
      char str[128];
      sprintf(str,"Cannot open input script %s",arg[0]);
      error->one(str);
    }
    infiles[nfile++] = infile;
  }
}

/* ---------------------------------------------------------------------- */

void Input::jump()
{
  if (narg < 1 || narg > 2) error->all("Illegal jump command");

  if (jump_skip) {
    jump_skip = 0;
    return;
  }

  if (me == 0) {
    if (infile != stdin) fclose(infile);
    infile = fopen(arg[0],"r");
    if (infile == NULL) {
      char str[128];
      sprintf(str,"Cannot open input script %s",arg[0]);
      error->one(str);
    }
    infiles[nfile-1] = infile;
  }

  if (narg == 2) {
    label_active = 1;
    if (labelstr) delete [] labelstr;
    int n = strlen(arg[1]) + 1;
    labelstr = new char[n];
    strcpy(labelstr,arg[1]);
  }
}

/* ---------------------------------------------------------------------- */

void Input::label()
{
  if (narg != 1) error->all("Illegal label command");
  if (label_active && strcmp(labelstr,arg[0]) == 0) label_active = 0;
}

/* ---------------------------------------------------------------------- */

void Input::log()
{
  if (narg != 1) error->all("Illegal log command");

  if (me == 0) {
    if (logfile) fclose(logfile);
    if (strcmp(arg[0],"none") == 0) logfile = NULL;
    else {
      logfile = fopen(arg[0],"w");
      if (logfile == NULL) {
	char str[128];
	sprintf(str,"Cannot open logfile %s",arg[0]);
	error->one(str);
      }
    }
    if (universe->nworlds == 1) universe->ulogfile = logfile;
  }
}

/* ---------------------------------------------------------------------- */

void Input::next_command()
{
  if (variable->next(narg,arg)) jump_skip = 1;
}

/* ---------------------------------------------------------------------- */

void Input::print()
{
  if (narg == 0) error->all("Illegal print command");

  // substitute for $ variables (no printing)
  // print args one at a time, separated by spaces

  for (int i = 0; i < narg; i++) {
    substitute(arg[i],0);
    if (me == 0) {
      if (screen) fprintf(screen,"%s ",arg[i]);
      if (logfile) fprintf(logfile,"%s ",arg[i]);
    }
  }

  if (me == 0) {
    if (screen) fprintf(screen,"\n");
    if (logfile) fprintf(logfile,"\n");
  }
}

/* ---------------------------------------------------------------------- */

void Input::variable_command()
{
  variable->set(narg,arg);
}

/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */
/* ---------------------------------------------------------------------- */

/* ----------------------------------------------------------------------
   one function for each APP-specific input script command
------------------------------------------------------------------------- */
