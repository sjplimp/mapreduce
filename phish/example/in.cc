# CC algorithm

sp 1 ../apps/readgraph tmp.graph
sp 1 ../apps/rmatgen 10 a b c d frac seed
sp 2 ../apps/graph
sp 3 ../apps/trigger tmp.trigger # could be every 10 secs

connect 1 one2many/direct 2
connect 2 ring 2
connect 3 one2one 2

layout 1 1
layout 2 10

