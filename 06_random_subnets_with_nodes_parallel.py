#!/usr/bin/env python

import os, random
from multiprocessing import cpu_count, Pool

total_loops = 100000
step = 5000
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


def generate_subnets(loop, *argv):

    fsub = open('subnets-' + str(loop) + ".csv", "a")
    fsub.write('subnetID:ID\n')
    frel = open('relationships-' + str(loop) + ".csv", "a")    
    frel.write(":START_ID,:END_ID,:TYPE\n")
    fip = open('ipaddresses-' + str(loop) + ".csv", "a")
    fip.write('ip_addr:ID,ip_num\n')
    ip_list = []

    while loop > 0 :
        loop -= 1

        a = random.randint(1,254)
        b = random.randint(1,254)
        c = random.randint(1,254)
        d = random.randint(1,254)
        mask = subnets[random.randint(0,len(subnets)-1)]

        ip_int = int(format_binary(a) + format_binary(b) + format_binary(c) + format_binary(d), 2)
        wildcard = 32 - mask
        network_mask = int('1' * mask + '0' * wildcard, 2)
        broadcast_mask = int('0' * mask + '1' * wildcard, 2)
        low_ip = ip_int & network_mask
        high_ip = ip_int | broadcast_mask
    
        ip = "%s.%s.%s.%s" % (a, b, c, d)
        network_str = ip + "/" + str(mask)
        fsub.write(network_str + '\n')
    
        for i in range(low_ip, high_ip):
            my_ip = int2ip(i)
            frel.write('"' + network_str + '","' + my_ip + '",' + 'INCLUDES\n')
            #if i not in ip_list:
            fip.write(my_ip + "," + str(i) + "\n")
            #    ip_list.append(i)
    
    fsub.close()
    frel.close()
    fip.close()


pool = Pool(processes=cpu_count()-1) or 1
print pool

loops = total_loops
while loops > 0:
    pool.apply_async(generate_subnets, args=(loops, None))
    loops -= step

pool.close()
pool.join()

#merge files, keep uniq lines
print "merging files for uniq lines...",
for master in ['ipaddresses', 'subnets', 'relationships']:
    print "\t" + master + "...",
    loops = total_loops
    f = open(master + '.csv', "a")
    lines_seen = set()
    while loops > 0:
        filename = master + '-' + str(loops) + ".csv"
        try:
            for line in open(filename, "r"):
                if line not in lines_seen:
                    f.write(line)
                    lines_seen.add(line) 
            os.unlink(filename)
        except:
            pass
        loops -= step
    f.close()
print "done"
