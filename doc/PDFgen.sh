#!/bin/csh
# generate a PDF version of Manual

txt2html -b *.txt >! mapreduce.html

/home/sjplimp/tools/htmldoc/bin/htmldoc --title --toctitle "Table of Contents" --tocfooter ..i --toclevels 4 --header ... --footer ..1 --size letter --linkstyle plain --linkcolor blue -f mapreduce.pdf mapreduce.html

txt2html *.txt >! mapreduce.html
