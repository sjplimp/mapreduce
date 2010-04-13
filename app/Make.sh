# Make.sh = update Makefile.lib or Makefile.list or style_*.h files
# Syntax: sh Make.sh style
#         sh Make.sh Makefile.lib
#         sh Make.sh Makefile.list

# function to create one style_*.h file

style () {
  list=`grep -l $1 $2*.h`
  if (test -e style_$3.tmp) then
    rm -f style_$3.tmp
  fi
  for file in $list; do
    qfile="\"$file\""
    echo "#include $qfile" >> style_$3.tmp
  done
  if (test ! -e style_$3.tmp) then
    rm -f style_$3.h
    touch style_$3.h
  elif (test ! -e style_$3.h) then
    mv style_$3.tmp style_$3.h
    rm -f Obj_*/$4.d
  elif (test "`diff --brief style_$3.h style_$3.tmp`" != "") then
    mv style_$3.tmp style_$3.h
    rm -f Obj_*/$4.d
  else
    rm -f style_$3.tmp
  fi
}

# create individual style files
# called by "make machine"

if (test $1 = "style") then

  style COMMAND_CLASS   ""          command    input
  style MAP_CLASS       ""          map        callback
  style REDUCE_CLASS    ""          reduce     callback
  style HASH_CLASS      ""          hash       callback
  style COMPARE_CLASS   ""          compare    callback

# edit Makefile.lib
# called by "make makelib"
# use current list of *.cpp and *.h files in src dir w/out main.cpp

elif (test $1 = "Makefile.lib") then

  list=`ls -1 *.cpp | sed s/^main\.cpp// | tr "[:cntrl:]" " "`
  sed -i -e "s/SRC =	.*/SRC =	$list/" Makefile.lib
  list=`ls -1 *.h | tr "[:cntrl:]" " "`
  sed -i -e "s/INC =	.*/INC =	$list/" Makefile.lib

# edit Makefile.list
# called by "make makelist"
# use current list of *.cpp and *.h files in src dir

elif (test $1 = "Makefile.list") then

  list=`ls -1 *.cpp | tr "[:cntrl:]" " "`
  sed -i -e "s/SRC =	.*/SRC =	$list/" Makefile.list
  list=`ls -1 *.h | tr "[:cntrl:]" " "`
  sed -i -e "s/INC =	.*/INC =	$list/" Makefile.list

fi
