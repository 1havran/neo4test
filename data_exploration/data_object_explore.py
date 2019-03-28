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


There will be one or more cells within the row to be considered as pivot(s).
Pivot(s) will link to other cells in the same row. Multiple pivots are needed
for data with low quality:
  first row - first 5 columns have values, but not last 5
  second row - first 5 columns have no value, but last 5 have

There is also an option to use autopivots. Autopivots will find best pivots
in terms of line column coverage.
  In above example, there will be two pivots. id=1 (first column, to cover
  first 5 columns) and id=6 (to cover last 5 columns)

Multiple CSV files should be connected together, otherwise there will be
multiple not-connected graphs. Interconnection is done for all cells in a way
that all objects (cells) of the same VALUE but different TYPE are connected.
   e.g. TYPE=id with VALUE=24 connected with TYPE=age with VALUE=24.

Explore all shortest paths since there can be phantom connections that do not
really exist and can lead to fake results.
"""

from csv import reader as csvreader
from multiprocessing import cpu_count, Pool
from os import path, walk, unlink
from re import sub as resub
from string import printable
from hashlib import new as hashlib

SPLITTER = "___"
O_FILE_PREFIX = "o"
R_FILE_PREFIX = "r"
P_FILE_PREFIX = "p"
SEP = ","
VERBOSE = False


def clean_data(my_string):
    '''
    clean_data cleans my_string argument. It removes non-printable characters,
    specific csv characters and replaces empty chars in beginning and end.
    It also removes 'spliiter', specific string used for objects and relations.
    '''

    rexes = ['^ *', ' *$', SPLITTER]
    char_to_remove = ["'", '"', ',']
    clean_s = ''
    for char in my_string:
        if char in printable:
            if char not in char_to_remove:
                clean_s += char
    for regex in rexes:
        clean_s = resub(regex, '', clean_s)
    return clean_s


def log_me(string):
    ''' print string to STDOUT '''
    print(string)


def get_pivots(csv_file, pivots=['name', 'address'], delim=",", \
    whitelist=['*'], enc="utf-8", omit_empty_nodes=True, autopivot=False):
    '''
    get_pivots reads csv_file and returns a set of indexes for one or more pivots.
    Two diffrent CSVs are linked together via pivots.
    Pivot is a column index that is used to connect cells within same CSV row.
    If a cell for defined pivot is empty, then data in the rows might be lost.
    Autopivot tries to identify best pivots automatically.
    First row of the csv_file is considered as a header.
    whitelist is a list for whitelisted header fields.
    '''

    user_pivot_idx = set()
    header_map = {}
    line_reads = 0

    with open(csv_file, "r", encoding=enc) as csvfile:
        data = csvreader(csvfile, delimiter=delim)
        for row in data:
            for row_id, row_value in enumerate(row):

                data = clean_data(row_value)
                if omit_empty_nodes and data == "":
                    continue

                if line_reads < 1:
                    # ignore non whitelisted fields
                    for wl_field in whitelist:
                        if wl_field in ('*', data):
                            header_map[row_id] = []
                        if data in pivots:
                            user_pivot_idx.add(row_id)
                else:
                    if row_id not in header_map.keys():
                        continue
                    header_map[row_id].append(line_reads)

            line_reads += 1

    if not autopivot:
        got_all = len(pivots) == len(user_pivot_idx) <= header_map
        log_me("CSVFile: {}, AutoPivot: {}, Got all pivots: {}, \
            UserPivot IDs: {}"\
            .format(csv_file, autopivot, got_all, user_pivot_idx))
        return got_all, user_pivot_idx

    #otherwise autopivot
    #log_me((header_map)

    required_keys = set(header_map.keys())
    length = len(required_keys)
    line_coverage = set()
    pivot_idx = set()

    # find such pivots that cover most of the columns with values
    while required_keys and header_map:
        idx = algo_pick_longest(header_map)
        for item in header_map[idx]:
            if item not in line_coverage:
                pivot_idx.add(idx)
                line_coverage.add(item)
                if idx in required_keys:
                    required_keys.remove(idx)

        header_map.pop(idx)

    got_all = length == len(line_coverage) > 0
    log_me("CSVFile: {}, AutoPivot: {}, Got all: {}, UserPivot IDs: {}, \
         AutoPivot IDs: {}"\
         .format(csv_file, autopivot, got_all, user_pivot_idx, pivot_idx))
    return got_all, pivot_idx


def algo_pick_longest(header_map):
    ''' algo_pick_longest will return a index of the longest list in the dict '''
    idx = -1
    length = -1
    for k in header_map.keys():
        if len(header_map[k]) > length:
            idx = k
            length = len(header_map[k])
    return idx


def get_objects_and_rel_from_csv(csv_file, pivots, delim=",", whitelist=['*'], \
    enc="utf-8", omit_empty_nodes=True, autopivot=False):
    '''
    get_objects_and_rel_from_csv will read csv_file with header and creates
    unique objects and relations.
    Unique object is considered as a header label + cell value
    Relations are created within same row between pivots and non-pivots fields.
    Pivots is a list of strings - fields in csv header.
    Whitelist is a list of strings, fields in csv header.
    '''

    line_reads = 0
    header_map = {}
    object_map = {}
    relations_map = {}
    whitelist_field_ids = set()

    _, pivot_idx = get_pivots(csv_file=csv_file, pivots=pivots, \
                                    delim=delim, whitelist=whitelist, \
                                    enc=enc, omit_empty_nodes=omit_empty_nodes, \
                                    autopivot=autopivot)

    with open(csv_file, encoding=enc) as csvfile:
        data = csvreader(csvfile, delimiter=delim)
        for row in data:
            line_reads += 1

            for row_id, row_value in enumerate(row):
                data = clean_data(row_value)

                if omit_empty_nodes and data == "":
                    continue

                #header, not data
                if line_reads == 1:
                    # ignore non whitelisted fields
                    for wl_field in whitelist:
                        if wl_field in ('*', data):

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


def algo_get_hash(string, algo="sha1", encoding="utf-8"):
    ''' algo_get_hash will return a sha1 hash of string '''
    my_hash = hashlib(algo)
    my_hash.update(string.encode(encoding))
    return my_hash.hexdigest()


def gen_uuid_for_object(object_name, object_value):
    '''
    gen_uuid_for_object will generate unique id for the object in the graph.
    name of object is tight with csv header field and cell value joined by SPLITTER.
    '''
    my_hash = algo_get_hash(object_name + SPLITTER + object_value)
    return my_hash


def print_obj_rel(header_map, object_map, relations_map):
    ''' print_obj_rel will print parsed objects and relations from csv file '''
    # print objects
    for i in header_map:
        log_me("{},{}".format("object_id", header_map[i]))
        for element in object_map[i]:
            sha_id = gen_uuid_for_object(header_map[i], element)
            log_me(sha_id + element)
        log_me("")

    # print relations
    for pivot_id in relations_map.keys():
        for pivot_data in relations_map[pivot_id]:
            p_uuid = gen_uuid_for_object(header_map[pivot_id], pivot_data)

            for row_id in relations_map[pivot_id][pivot_data]:
                log_me("{} {} {}".format(header_map[pivot_id], SPLITTER, header_map[row_id]))
                for data in relations_map[pivot_id][pivot_data][row_id]:
                    d_uuid = gen_uuid_for_object(header_map[row_id], data)
                    log_me("{} {} {}".format(pivot_data, SPLITTER, data))
                    log_me("{} {} {}".format(p_uuid, SPLITTER, d_uuid))
                    log_me("")


def write_obj_rel(header_map, object_map, relations_map, suffix=".csv", folder="./"):
    '''
    write_obj_rel will write parsed objects and relations into the files.
    there will be two files created - object file and realtion file.
    '''

    object_file = O_FILE_PREFIX + SPLITTER + suffix
    with open(path.join(folder, object_file), "w") as obj_file:
        obj_file.write('{0}{1}{2}{1}{3}{4}'.format(":ID", SEP, "TYPE", "VALUE", "\n"))
        for i in header_map:
            for element in object_map[i]:
                sha_id = gen_uuid_for_object(header_map[i], element)
                obj_file.write(sha_id + SEP + str(header_map[i]) + SEP + element + "\n")

    rel_file = R_FILE_PREFIX + SPLITTER + suffix
    with open(path.join(folder, rel_file), "w") as rel_file:
        rel_file.write("{0}{1}{2}{1}{3}{4}".format(":START_ID", SEP, ":END_ID", ":TYPE", "\n"))
        for pivot_id in relations_map.keys():
            for pivot_data in relations_map[pivot_id]:
                p_uuid = gen_uuid_for_object(header_map[pivot_id], pivot_data)
                for row_id in relations_map[pivot_id][pivot_data]:
                    for data in relations_map[pivot_id][pivot_data][row_id]:
                        d_uuid = gen_uuid_for_object(header_map[row_id], data)
                        rel_file.write('{0}{1}{2}{1}{3}{4}'.format(p_uuid, \
                                                                   SEP, \
                                                                   d_uuid, \
                                                                   "HAS", \
                                                                   "\n"))


def get_files_prefixes(fs_path="./input", suffix=".csv$"):
    '''
    get_files_prefixes will search in the path and returns all files that
    contains objects and relations for the graph
    '''

    files_prefix = {}
    for _, _, files in walk(fs_path):
        for name in files:
            base_name = resub(suffix, '', name)
            name_parts = base_name.split(SPLITTER)

            if name_parts[0] == O_FILE_PREFIX or name_parts[0] == R_FILE_PREFIX:
                if name_parts[0] not in files_prefix.keys():
                    files_prefix[name_parts[0]] = set()
                files_prefix[name_parts[0]].add(name)
    return files_prefix


def merge_files(prefix, files, fs_path, delete_single=True):
    '''
    merge_files will merge all object files into single object file with
    unique objects. Same is done for relations files.
    '''

    obj = set()
    new_name = prefix + SPLITTER + "merged.csv"
    obj_file = open(path.join(fs_path, new_name), "w")
    for object_file in files:
        my_path = path.join(fs_path, object_file)
        with open(my_path, "r") as non_merged_file:
            for line in non_merged_file:
                line = line.rstrip()
                if line != "" and line not in obj:
                    obj.add(line)
                    obj_file.write(line + "\n")
        if delete_single:
            unlink(my_path)
    obj_file.close()


def run_phase2(fs_path="./input"):
    '''
    run_phase2 is a skeleton call to search for object and relations
    files and each of them is merged. each csv_file from phase1 will have 1
    object file and 1 relations file. phase2 will merge all objects together
    to create just 1 object file in total. same happens with relations file.
    '''

    files_prefix = get_files_prefixes(fs_path)
    for graph_files in files_prefix:
        merge_files(graph_files, files_prefix[graph_files], fs_path)


def run_phase3(fs_path="./input"):
    '''
    run_phase3 is a skeleton call to search for object files and
    to connect_pivots
    '''

    files_prefix = get_files_prefixes(fs_path)
    if O_FILE_PREFIX in files_prefix.keys():
        connect_pivots(P_FILE_PREFIX, files_prefix[O_FILE_PREFIX], fs_path)


def connect_pivots(prefix, files, fs_path, suffix=".csv"):
    '''
    connect_pivots will connect objects having same value but different type (
    different csv header). If this func is called after run_phase2,
    there should be just one object file in files argument.
    '''

    obj = set()

    # find if any object of different type has the same value and join
    for object_file in files:
        my_path = path.join(fs_path, object_file)
        pivot_file = open(my_path, 'r')
        pivot_line_reads = 0
        for pivot_line in pivot_file:
            pivot_line_reads += 1
            # skip header
            if pivot_line_reads == 1:
                continue

            pivot_line = pivot_line.rstrip()
            pivot_id, pivot_type, pivot_value = pivot_line.split(SEP)

            for rel_file in files:
                rel_path = path.join(fs_path, rel_file)
                rel_line_reads = 0
                with open(rel_path, "r") as my_file:
                    for line in my_file:
                        rel_line_reads += 1
                        if rel_line_reads == 1:
                            continue

                        line = line.rstrip()
                        new_id, new_type, new_value = line.split(SEP)

                        if new_id == pivot_id or new_type == pivot_type:
                            continue

                        if new_value == pivot_value:
                            if pivot_type > new_type:
                                rel = pivot_id + SPLITTER + new_id
                            else:
                                rel = new_id + SPLITTER + pivot_id
                            if rel not in obj:
                                obj.add(rel)

        pivot_file.close()

    if obj:
        name = prefix + suffix
        pivot_file = open(path.join(fs_path, name), "w")
        pivot_file.write(":START_ID,:END_ID,:TYPE\n")

        for rel in obj:
            p_from, p_to = rel.split(SPLITTER)
            data = '{0}{1}{2}{1}{3}{4}'.format(p_from, SEP, p_to, 'RELATES', '\n')
            pivot_file.write(data)

        pivot_file.close()


def process_csv_file(csv_file="2007.csv", pivots=["FlightNum"], whitelist=['*'], \
                     autopivot=True):
    '''
    process_csv_file will generate object and relation files from CSV
    and writes them to disk
    '''

    header_map, object_map, relations_map = \
        get_objects_and_rel_from_csv(csv_file=csv_file, \
                                     pivots=pivots, \
                                     whitelist=whitelist, \
                                     autopivot=autopivot)
    write_obj_rel(header_map, object_map, relations_map, suffix=csv_file, \
                  folder="./input")

    if VERBOSE:
        print_obj_rel(header_map, object_map, relations_map)


def run_phase1():
    '''
    run_phase1 parses csv files concurently and generates object and relations
    files from each csv file. For each csv file, define list of pivots (column
    names), list of whitelisted columns (useful if CSV has 100+ fields),
    and autopivot. If it is True, list of pivots is ignored.
    '''

    if cpu_count() <= 1:
        pool = Pool(processes=1)
    else:
        pool = Pool(processes=cpu_count()-1)

    pool.apply_async(process_csv_file, \
        args=("2007-30.csv", \
              ['whatever_since_autopivot_is_true'], \
              ['*'], \
              True))
    #pool.apply_async(process_csv_file, \
    #    args=("2007.csv", \
    #          ['whatever_since_autopivot_is_true'], \
    #          ['*'], \
    #          True))
    pool.apply_async(process_csv_file, \
        args=("a.csv", \
        ['whatever_since_autopivot_is_true'], \
        ['*'], \
        True))
    pool.close()
    pool.join()


def main():
    '''
    main starts the show. there are 3 phases to generate vertices (objects) and
    edges (relations) for the graph (can be imported into neo4j)
    '''

    # generate objects and relations
    run_phase1()

    # merge objects and relations
    run_phase2(fs_path="./input")

    # connect objects of different types with same value
    run_phase3(fs_path="./input")


if __name__ == "__main__":
    main()
