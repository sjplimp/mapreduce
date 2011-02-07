#!/bin/csh
# generate a PDF version of Manual

python Manual.py
txt2html -b *.txt

/home/sjplimp/tools/htmldoc/bin/htmldoc --title --toctitle "Table of Contents" --tocfooter ..i --toclevels 4 --header ... --footer ..1 --size letter --linkstyle plain --linkcolor blue -f Manual.pdf Manual.html Section_build.txt Section_script.txt Section_functions.txt Section_commands.txt Section_errors.txt [a-z]*.txt

txt2html *.txt
