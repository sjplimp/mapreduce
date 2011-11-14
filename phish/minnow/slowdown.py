#!/usr/local/bin/python

import sys,time
from phish import Phish

def send(nvalues):
  global time_previous,delta
  elapsed = phish.timer() - time_previous
  if elapsed < delta: time.sleep(delta-elapsed)
  buf,len = phish.datum()
  phish.pack_datum(buf,len)
  phish.send(0)
  time_previous = phish.timer()

phish = Phish()

args = phish.init(sys.argv)
phish.input(0,send,None,1)
phish.output(0)
phish.check()

if len(args) != 1: phish.error("Slowdown syntax: slowdown delta")

delta = float(args[0])
time_previous = phish.timer()

phish.loop()
phish.exit()
