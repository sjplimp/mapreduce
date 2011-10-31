# ping-pong test between 2 processes

sp 1 ../apps/ping 1000000 0
sp 2 ../apps/pong

connect 1 one2one 2
connect 2 one2one 1

layout 1 1
layout 2 1
