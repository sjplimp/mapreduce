/* ----------------------------------------------------------------------
   APP
   contact info, copyright info, etc
------------------------------------------------------------------------- */

#ifndef APP_VARIABLE_H
#define APP_VARIABLE_H

#include "pointers.h"

namespace APP_NS {

class Variable : protected Pointers {
 public:
  Variable(class APP *);
  ~Variable();
  void set(int, char **);
  void set(char *, char *);
  int next(int, char **);
  int find(char *);
  int equalstyle(int);
  char *retrieve(char *);
  double compute_equal(int);
  int int_between_brackets(char *&);

 private:
  int me;
  int nvar;                // # of defined variables
  int maxvar;              // max # of variables arrays can hold
  char **names;            // name of each variable
  int *style;              // style of each variable
  int *num;                // # of values for each variable
  int *index;              // next available value for each variable
  char ***data;            // str value of each variable's values
  int precedence[7];       // precedence level of math operators

  struct Tree {            // parse tree for atom-style variables
    double value;
    double *array;
    int *iarray;
    int nstride;
    int type;
    Tree *left,*right;
  };

  void remove(int);
  void extend();
  void copy(int, char **, char **);
  double evaluate(char *, Tree **);
  double eval_tree(Tree *, int);
  void free_tree(Tree *);
  int find_matching_paren(char *, int, char *&);
  int math_function(char *, char *, Tree **, Tree **, int &, double *, int &);
  int object_function(char *, char *, Tree **, Tree **,
		      int &, double *, int &);
  int keyword(char *, double *);
};

}

#endif
