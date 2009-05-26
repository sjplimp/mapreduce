#!/bin/csh
# generate a PDF version of Manual

txt2html -b *.txt >! mapreduce.html

/home/sjplimp/tools/htmldoc/bin/htmldoc --title --toctitle "Table of Contents" --tocfooter ..i --toclevels 4 --header ... --footer ..1 --size letter --linkstyle plain --linkcolor blue -f Manual.pdf Manual.html background.html whatis.html start.html program.html interface_c++.html create.html copy.html destroy.html add.html aggregate.html clone.html collapse.html collate.html compress.html convert.html gather.html map.html reduce.html scrunch.html sort_keys.html sort_values.html sort_multivalues.html stats.html kv_add.html settings.html interface_c.html interface_python.html technical.html examples.html

txt2html *.txt >! mapreduce.html
