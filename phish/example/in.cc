# CC algorithm

minnow 1 readgraph tmp.graph
minnow 1 rmatgen 10 a b c d frac seed
minnow 2 graph
minnow 3 trigger tmp.trigger # could be every 10 secs

connect 1 one2many/direct 2
connect 2 ring 2
connect 3 one2one 2

layout 1 1
layout 2 10

