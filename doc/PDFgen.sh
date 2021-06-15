#!/bin/csh
# generate a PDF version of Manual

txt2html -b *.txt

htmldoc --title --toctitle "Table of Contents" --tocfooter ..i --toclevels 4 --header ... --footer ..1 --size letter --linkstyle plain --linkcolor blue -f Manual.pdf Manual.html Background.html Whatis.html Start.html Program.html Interface_c.html++ Interface_c.html Interface_python.html Interface_oink.html Technical.html Examples.html [a-z]*.html

txt2html *.txt
