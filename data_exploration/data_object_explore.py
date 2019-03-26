"""

data_object_explore.py will automatically create connected graph structure
 (vertices, edges) from CSV files. The output (objects -> vertices, relations
 -> edges) can be imported in Neo4j using offline command such as :

neo4j-admin import --mode=csv \
 --nodes o___merged.csv \
 --relationships=r__merged.csv \
 --relationships=p.csv

Each object/vertex has two properties: TYPE and VALUE,
  where TYPE is CSV header field and VALUE is a cell CSV value.

For graph exploration, use shortestPath algorithm to find the connection. e.g.:

MATCH (m {TYPE: "Origin", VALUE: "EKO"},
 (n {TYPE: "Origin", VALUE: "HPN"}),
 (l {TYPE:"FlightNum"}),
 p=shortestPath((m)-[*]-(n)),
 pp=shortestPath((n)-[*]-(l)),
RETURN m,n,p,pp

For graphs using autopivots, explore all shortest paths, since there can be
phantom connections that do not really exist and can lead to fake results.

"""

from multiprocessing import cpu_count, Pool
import csv
import hashlib
import os
import re
import string

splitter = "___"
o_file_prefix = "o"
r_file_prefix = "r"
p_file_prefix = "p"
sep = ","
verbose = False


# clean_data cleans my_string argument. It removes non-printable characters,
# specific csv characters and replaces empty chars in beginning and end.
# It also removes 'spliiter', specific string used for objects and relations.
def clean_data(my_string):
    rexes = ['^ *', ' *$', splitter]
    char_to_remove = ["'", '"', ',']
    clean_s = ''
    for c in my_string:
        if c in string.printable:
            if c not in char_to_remove:
                clean_s += c
    for regex in rexes:
        clean_s = re.sub(regex, '', clean_s)
    return clean_s


# get_pivots reads csv_file and returns a set of indexes for one or more pivots.
# Two diffrent CSVs are linked together via pivots.
# Pivot is a column index that is used to connect cells within same CSV row.
# If a cell for defined pivot is empty, then data in the rows might be lost.
# Autopivot tries to identify best pivots automatically.
# First row of the csv_file is considered as a header.
# whitelist is a list for whitelisted header fields.
def get_pivots(csv_file, pivots=['name','address'], delim=",", whitelist=['*'], enc="utf-8", omit_empty_nodes=True, autopivot=False):

    user_pivot_idx = set()
    header_map = {}
    line_reads = 0

    with open(csv_file, "r", encoding=enc) as csvfile:
        data = csv.reader(csvfile, delimiter=delim)
        for row in data:
            for row_id in range(len(row)):

                data = clean_data(row[row_id])
                if omit_empty_nodes and data == "":
                    continue

                if line_reads < 1:
                    # ignore non whitelisted fields
                    for wl_field in whitelist:
                        if wl_field == '*' or wl_field == data:
                            header_map[row_id] = []
                        if data in pivots:
                            user_pivot_idx.add(row_id)
                else:
                    if row_id not in header_map.keys():
                        continue
                    header_map[row_id].append(line_reads)

            line_reads += 1

    if not autopivot:
        got_all = len(pivots) == len(user_pivot_idx) <= len(header_map.keys()) > 0
        print ("CSVFile: {}, AutoPivot: {}, Got all pivots: {}, UserPivot IDs: {}".format(csv_file, autopivot, got_all, user_pivot_idx))
        return got_all, user_pivot_idx

    #otherwise autopivot
    #print (header_map)

    required_keys = set(header_map.keys())
    l = len(required_keys)
    line_coverage = set()
    pivot_idx = set()

    # find such pivots that cover most of the columns with values
    while len(required_keys) > 0 and len(header_map) > 0:
        idx = algo_pick_longest(header_map)
        for item in header_map[idx]:
            if item not in line_coverage:
                pivot_idx.add(idx)
                line_coverage.add(item)
                if idx in required_keys:
                    required_keys.remove(idx)

        header_map.pop(idx)

    got_all = l == len(line_coverage) > 0
    print ("CSVFile: {}, AutoPivot: {}, Got all: {}, UserPivot IDs: {},  AutoPivot IDs: {}".format(csv_file, autopivot, got_all, user_pivot_idx, pivot_idx))
    return got_all, pivot_idx


# algo_pick_longest will return a index of the longest list in the dict
def algo_pick_longest(map):
    idx = -1
    length = -1
    for k in map.keys():
        if len(map[k]) > length:
            idx = k
            length = len(map[k])
    return idx


# get_objects_and_rel_from_csv will read csv_file with header and creates
# unique objects and relations.
# Unique object is considered as a header label + cell value
# Relations are created within same row between pivots and non-pivots fields.
# Pivots is a list of strings - fields in csv header.
# Whitelist is a list of strings, fields in csv header.
def get_objects_and_rel_from_csv(csv_file, pivots, delim=",", whitelist=['*'], enc="utf-8", omit_empty_nodes=True, autopivot=False):

    line_reads = 0
    header_map = {}
    object_map = {}
    relations_map = {}

    whitelist_field_ids = set()

    got_all, pivot_idx = get_pivots(csv_file=csv_file, pivots=pivots, \
                                    delim=delim, whitelist=whitelist, \
                                    enc=enc, omit_empty_nodes=omit_empty_nodes, \
                                    autopivot=autopivot)

    with open(csv_file, encoding=enc) as csvfile:
        data = csv.reader(csvfile, delimiter=delim)
        for row in data:
            line_reads += 1

            for row_id in range(len(row)):
                data = clean_data(row[row_id])

                if omit_empty_nodes and data == "":
                    continue

                #header, not data
                if line_reads == 1:
                    # ignore non whitelisted fields
                    for wl_field in whitelist:
                        if wl_field == '*' or wl_field == data:

                            whitelist_field_ids.add(row_id)
                            header_map[row_id] = data
                            object_map[row_id] = set()

                            #each pivot refers to other non-pivot fields
                            for pivot_id in pivot_idx:
                                if row_id != pivot_id:
                                    relations_map[pivot_id] = {}

                #data row, not header
                else:
                    if row_id not in whitelist_field_ids:
                        continue
                    if data not in object_map[row_id]:
                        object_map[row_id].add(data)

                    # don't create relations between pivots
                    if row_id in pivot_idx:
                        continue

                    for pivot_id in pivot_idx:
                        pivot_data = clean_data(row[pivot_id])
                        if omit_empty_nodes and pivot_data == "":
                            continue

                        if pivot_data not in relations_map[pivot_id].keys():
                            relations_map[pivot_id][pivot_data] = {}
                        if row_id not in relations_map[pivot_id][pivot_data].keys():
                            relations_map[pivot_id][pivot_data][row_id] = set()
                        # each pivot within the row refers to all other data within the row
                        relations_map[pivot_id][pivot_data][row_id].add(data)

    return header_map, object_map, relations_map


#algo_get_hash will return a sha1 hash of string s
def algo_get_hash(s, algo="sha1", encoding="utf-8"):
    h = hashlib.new(algo)
    h.update(s.encode(encoding))
    return h.hexdigest()


# gen_uuid_for_object will generate unique id for the object in the graph.
# name of object is tight with csv header field and cell value joined by splitter.
def gen_uuid_for_object(object_name, object_value, splitter=splitter):
    hash = algo_get_hash(object_name + splitter + object_value)
    return hash


# print_obj_rel will print parsed objects and relations from csv file
def print_obj_rel(header_map, object_map, relations_map):

    # print objects
    for i in header_map:
        # print object_name
        print ("{},{}".format("object_id",header_map[i]))
        for element in object_map[i]:
            sha_id = gen_uuid_for_object(header_map[i], element)
            print (sha_id, element)
        print ("")

    # print relations
    for pivot_id in relations_map.keys():
        for pivot_data in relations_map[pivot_id]:
            p_uuid = gen_uuid_for_object(header_map[pivot_id], pivot_data)

            for row_id in relations_map[pivot_id][pivot_data]:
                print ("{} {} {}".format(header_map[pivot_id], splitter, header_map[row_id]))
                for data in relations_map[pivot_id][pivot_data][row_id]:
                    d_uuid = gen_uuid_for_object(header_map[row_id], data)
                    print ("{} {} {}".format(pivot_data, splitter, data))
                    print ("{} {} {}".format(p_uuid, splitter, d_uuid))
                    print ("")


# write_obj_rel will write parsed objects and relations into the files.
# there will be two files created - object file and realtion file.
def write_obj_rel(header_map, object_map, relations_map, suffix=".csv", dir="./"):
    object_file = o_file_prefix + splitter + suffix
    with open(os.path.join(dir, object_file), "w") as f:
        f.write('{}{}{}{}{}{}'.format(":ID", sep, "TYPE", sep, "VALUE", "\n"))
        for i in header_map:
            for element in object_map[i]:
                sha_id = gen_uuid_for_object(header_map[i], element)
                f.write(sha_id + sep + str(header_map[i]) + sep + element + "\n")

    rel_file = r_file_prefix + splitter + suffix
    with open(os.path.join(dir, rel_file), "w") as f:
        f.write("{}{}{}{}{}{}".format(":START_ID",sep,":END_ID", sep, ":TYPE", "\n"))
        for pivot_id in relations_map.keys():
            for pivot_data in relations_map[pivot_id]:
                p_uuid = gen_uuid_for_object(header_map[pivot_id], pivot_data)
                for row_id in relations_map[pivot_id][pivot_data]:
                    for data in relations_map[pivot_id][pivot_data][row_id]:
                        d_uuid = gen_uuid_for_object(header_map[row_id], data)
                        f.write('{}{}{}{}{}{}'.format(p_uuid, sep, d_uuid, sep, "HAS", "\n"))


# get_files_prefixes will search in the path and returns all files that
# contains objects and relations for the graph
def get_files_prefixes(path="./input", suffix=".csv$"):
    files_prefix = {}
    for root, dirs, files in os.walk(path):
        my_files = []
        for name in files:
            base_name = re.sub(suffix, '', name)
            name_parts = base_name.split(splitter)

            if name_parts[0] == o_file_prefix or name_parts[0] == r_file_prefix:
                if name_parts[0] not in files_prefix.keys():
                    files_prefix[name_parts[0]] = set()
                files_prefix[name_parts[0]].add(name)
    return files_prefix


# merge_files will merge all object files into single object file with
# unique objects. Same is done for relations files.
def merge_files(prefix, files, path, delete_single=True):
    obj = set()
    new_name = prefix + splitter + "merged.csv"
    f = open(os.path.join(path, new_name), "w")
    for object_file in files:
        my_path = os.path.join(path, object_file)
        with open(my_path, "r") as ff:
            for line in ff:
                line = line.rstrip()
                if line != "" and line not in obj:
                    obj.add(line)
                    f.write(line + "\n")
        if delete_single:
            os.unlink(my_path)
    f.close()


# run_phase2 is a skeleton call to search for object and relations
# files and each of them is merged. each csv_file from phase1 will have 1
# object file and 1 relations file. phase2 will merge all objects together
# to create just 1 object file in total. same happens with relations file.
def run_phase2(path="./input"):
    files_prefix = get_files_prefixes(path)
    for f in files_prefix.keys():
        merge_files(f, files_prefix[f], path)


# run_phase3 is a skeleton call to search for object files and
# to connect_pivots
def run_phase3(path="./input"):
    files_prefix = get_files_prefixes(path)
    if o_file_prefix in files_prefix.keys():
        connect_pivots(p_file_prefix, files_prefix[o_file_prefix], path)


# connect_pivots will connect objects having same value but different type (
# different csv header). If this func is called after run_phase2,
# there should be just one object file in files argument.
def connect_pivots(prefix, files, path, suffix=".csv"):

    obj = set()

    # find if any object of different type has the same value and join
    for object_file in files:
        my_path = os.path.join(path, object_file)
        pivot_file = open(my_path, 'r')
        pivot_line_reads = 0
        for pivot_line in pivot_file:
            pivot_line_reads += 1
            # skip header
            if pivot_line_reads == 1:
                continue

            pivot_line = pivot_line.rstrip()
            pivot_id, pivot_type, pivot_value = pivot_line.split(sep)

            for rel_file in files:
                rel_path = os.path.join(path, rel_file)
                rel_line_reads = 0
                with open(rel_path, "r") as ff:
                    for line in ff:
                        rel_line_reads += 1
                        if rel_line_reads == 1:
                            continue

                        line = line.rstrip()
                        new_id, new_type, new_value = line.split(sep)

                        if new_id == pivot_id or new_type == pivot_type:
                            continue

                        if new_value == pivot_value:
                            if pivot_type > new_type:
                                rel = pivot_id + splitter + new_id
                            else:
                                rel = new_id + splitter + pivot_id
                            if rel not in obj:
                                obj.add(rel)

        pivot_file.close()

    if len(obj) > 0:
        name = prefix + suffix
        pivot_file = open(os.path.join(path, name), "w")
        pivot_file.write(":START_ID,:END_ID,:TYPE\n")

        for o in obj:
            p_from, p_to = o.split(splitter)
            str = '{}{}{}{}{}{}'.format(p_from, sep, p_to, sep, 'RELATES', '\n')
            pivot_file.write(str)

        pivot_file.close()


# process_csv_file will generate object and relation files from CSV and writes them to disk
def process_csv_file(csv_file="2007.csv", pivots=["FlightNum"], whitelist=['*'], autopivot=True):
    header_map, object_map, relations_map = get_objects_and_rel_from_csv(csv_file=csv_file, pivots=pivots, whitelist=whitelist, autopivot=autopivot)
    write_obj_rel(header_map, object_map, relations_map, suffix=csv_file, dir="./input")

    if verbose:
        print_obj_rel(header_map, object_map, relations_map)


# run_phase1 parses csv files concurently and generates object and relations
# files from each csv file.
def run_phase1():
    if cpu_count() <=1:
        pool = Pool(processes=1)
    else:
        pool = Pool(processes=cpu_count()-1)

    pool.apply_async(process_csv_file, args=("2007-30.csv", ['whatever_since_autopivot_is_true'], ['*'], True))
    pool.apply_async(process_csv_file, args=("2007.csv", ['whatever_since_autopivot_is_true'], ['*'], True))
    pool.apply_async(process_csv_file, args=("a.csv", ['whatever_since_autopivot_is_true'], ['*'], True))
    pool.close()
    pool.join()


# main starts the show. there are 3 phases to generate vertices (objects) and
# edges (relations) for the graph (can be imported into neo4j)
def main():
    # generate objects and relations
    run_phase1()

    # merge objects and relations
    run_phase2(path="./input")

    # connect objects of different types with same value
    run_phase3(path="./input")


if __name__ == "__main__":
    main()
