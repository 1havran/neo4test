#!/usr/bin/env python

import os
from multiprocessing import cpu_count, Pool

def generate_ipv4(low_range,high_range):
	f = open('file-' + str(low_range) + '_' + str(high_range) + ".csv", "a")	
	f.write("ip_addr:ID,ip_num:int\n")
	for a in range(low_range,high_range):
		for b in range (0,256):
			for c in range(0,256):
				for d in range(0,256):
					ip_addr = "%s.%s.%s.%s" % (a, b, c, d)
					ip_num = a*256**3 + b*256**2 + c*256 + d
					f.write(ip_addr + "," + str(ip_num) + "\n")
	f.close()

num_of_processes = cpu_count()-1
chunk_factor = 1
pool = Pool(processes=num_of_processes)
print pool

left = 256
step = left / (num_of_processes * chunk_factor)
while left > step:
	high = left
	left -= step
	low = left
	pool.apply_async(generate_ipv4, args=(low, high))
else:
	pool.apply_async(generate_ipv4, args=(0, left))

pool.close()
pool.join()


