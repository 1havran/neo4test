#!/usr/bin/env python

import os, random

loops = 300000
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

    ip = "%s.%s.%s.%s" % (a, b, c, d)
    ip_int = int(format_binary(a) + format_binary(b) + format_binary(c) + format_binary(d), 2)
    mask = subnets[random.randint(0,len(subnets)-1)]
    wildcard = 32 - mask
    network_mask = int('1' * mask + '0' * wildcard, 2)
    broadcast_mask = int('0' * mask + '1' * wildcard, 2)
    low_ip = ip_int & network_mask
    high_ip = ip_int | broadcast_mask

    network_str = ip + "/" + str(mask)
    fsub.write(network_str + '\n')

    for i in range(low_ip, high_ip):
        my_ip = int2ip(i)
        frel.write('"' + network_str + '","' + my_ip + '",' + 'INCLUDES\n')
        if i not in ip_list:
            fip.write(my_ip + "," + str(i) + "\n")
            ip_list.append(i)

fsub = open('subnets-' + str(loops) + ".csv", "a")
fsub.write('subnetID:ID\n')
frel = open('relationships-' + str(loops) + ".csv", "a")    
frel.write(":START_ID,:END_ID,:TYPE\n")
fip = open('ipaddresses-' + str(loops) + ".csv", "a")
fip.write('ip_addr:ID,ip_num\n')
ip_list = []

while loops > 0:
    loops -= 1
    generate_subnets()

fsub.close
frel.close
fip.close
