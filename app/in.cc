# connected-component identification

variable order equal 15
variable nz equal 2
variable nrows equal 2^v_order
variable n equal v_nrows*v_nz

mre = mrmpi()
#mre.memsize(1)
mre.verbosity(1)
mre.timer(1)

rmat(mre,$n,${nrows},0.25,0.25,0.25,0.25,0.0,12345)

mrv = mrmpi()
#mrv.memsize(1)
mrv.verbosity(1)
mrv.timer(1)

mrz = mrmpi()
#mrz.memsize(1)
mrz.verbosity(1)
mrz.timer(1)

#cc(${nrows},mre,mrv,mrz)
cc2(${nrows},0,mre,mrv,mrz)

# print CC stats, destroys mrv

inv = map.invert()
mrv.map_mr(mrv,inv)
mrv.collate()
sum = reduce.sum()
mrv.reduce(sum)
mrv.map_mr(mrv,inv)
mrv.collate()
mrv.reduce(sum)
mrv.gather(1)
ic = compare.intcmp()
mrv.sort_keys(ic)
mrv.print(0,1,1,1)

mre.delete()
mrv.delete()
mrz.delete()
