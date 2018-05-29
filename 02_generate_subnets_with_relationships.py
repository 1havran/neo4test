#!/usr/bin/env python

import os, random

loops = 30000
subnets = [23, 24, 25, 26, 27, 28, 29]

def format_binary(num):
	return format(num, '08b')

def int2ip(i):
	d = i % 256
	i = i / 256
	c = i % 256
	i = i / 256
	b = i % 256
	a = i / 256
	return "%s.%s.%s.%s" % (a, b, c, d)

def generate_subnets():
	a = random.randint(1,254)
	b = random.randint(1,254)
	c = random.randint(1,254)
	d = random.randint(1,254)
	ip = int(format_binary(a) + format_binary(b) + format_binary(c) + format_binary(d), 2)
	mask = subnets[random.randint(0,len(subnets)-1)]
	wildcard = 32 - mask
	network_mask = int('1'*mask + '0'*wildcard, 2)
	broadcast_mask = int('0'*mask + '1'*wildcard, 2)
	low_ip = ip & network_mask
	high_ip = ip | broadcast_mask
	network_str = int2ip(ip) + "/" + str(mask)

	fsub.write(network_str + ',' + int2ip(ip) + ',' + str(mask) + '\n')

	for ip in range(low_ip, high_ip):
		frel.write(network_str + ',' + int2ip(ip) + ',' + 'BELONG_TO\n')


fsub = open('subnets-' + str(loops) + ".csv", "a")
fsub.write('subnetID:ID,network,mask:int\n')
frel = open('relationships-' + str(loops) + ".csv", "a")	
frel.write(":START_ID,:END_ID,:TYPE\n")

while loops > 0:
	loops -= 1
	generate_subnets()

fsub.close
frel.close

