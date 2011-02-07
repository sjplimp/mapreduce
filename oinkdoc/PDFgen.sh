#!/bin/csh
# generate a PDF version of Manual

python Manual.py
txt2html -b *.txt

/home/sjplimp/tools/htmldoc/bin/htmldoc --title --toctitle "Table of Contents" --tocfooter ..i --toclevels 4 --header ... --footer ..1 --size letter --linkstyle plain --linkcolor blue -f Manual.pdf Manual.html Section_build.html Section_script.html Section_functions.html Section_commands.html Section_errors.html [a-z]*.html

txt2html *.txt
