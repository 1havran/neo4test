import csv
import os

def get_objects_and_rel_from_csv(csv_file, pivot="", delim=",", whitelist=['*']):
    line_reads = 0
    header = []
    pivot_id = 0
    object_files = []
    relationships_files = []
    objects = []
    relationships = []
    whitelist_field_ids = set()

    with open(csv_file) as csvfile:
        data = csv.reader(csvfile, delimiter=delim)
        for row in data:
            line_reads += 1
            if line_reads == 1:
                header = row
                for i in range(len(header)):

                    is_valid_field = False

                    for wl_field in whitelist:
                        if row[i] == wl_field:
                            whitelist_field_ids.add(i)
                            is_valid_field = True
                            if row[i] == pivot:
                                pivot_id = i


                    name = "o__" + str(row[i]) + "__" + csv_file
                    print(name)
                    new_file = ''
                    if is_valid_field:
                        new_file = open(name, "w")
                        new_file.write(row[i] + ":value(" + str(row[i]) + "-ID)\n")
                    object_files.append(new_file)
                    objects.append(set())


                    rel_file = ''
                    if is_valid_field and i != pivot_id:
                        rel_name = "r__" + pivot + "__" + str(row[i]) + "__" + csv_file
                        print (rel_name)
                        rel_file = open(rel_name, "w")
                        rel_file.write(":START_ID("+ str(pivot) + "-ID),:END_ID("+ str(row[i])+"-ID),:TYPE\n")
                    relationships.append(set())
                    relationships_files.append(rel_file)

            else:
                for i in range(len(row)):
                    if i not in whitelist_field_ids:
                        continue
                    if row[i] not in objects[i]:
                        objects[i].add(row[i])
                    if i != pivot_id:
                        rel = '"' + str(row[pivot_id]) + '","' + str(row[i]) + '",HAS'
                        if rel not in relationships[i]:
                           relationships[i].add(rel)

    for i in range(len(header)):
        if i not in whitelist_field_ids:
            continue

        for element in objects[i]:
            object_files[i].write(element + "\n")
        object_files[i].close()

    for i in range(len(relationships)):
        if i not in whitelist_field_ids:
            continue

        for element in relationships[i]:
            relationships_files[i].write(element + "\n")
        try:
            relationships_files[i].close()
        except:
            #string here
            pass


def remove_duplicates(path="./"):

    objects = {}
    relationships = {}

    for root, dirs, files in os.walk(path):
        my_files = []
        for name in files:
            name_parts = name.split("__")
            if name_parts[-1] == '__merged.csv':
                continue

            my_files.append(name)
            if name_parts[0] == 'o':
                if name_parts[1] not in objects.keys():
                    objects[name_parts[1]] = [name]
                else:
                    objects[name_parts[1]].append(name)

            if name_parts[0] == 'r':
                rel = name_parts[1] + "__" + name_parts[2]
                if rel not in relationships.keys():
                    relationships[rel] = [name]
                else:
                    relationships[rel].append(name)


    for field in objects.keys():
        obj = set()
        new_name = "o__" + str(field) + "__merged.csv"
        f = open(new_name, "w")
        for object_file in objects[field]:
            with open(object_file, "r") as ff:
                line = ff.read()
                if line != "" and line not in obj:
                    obj.add(line)
                    f.write(line)
        f.close()

    for field in relationships.keys():
        rel = set()
        new_name = "r__" + str(field) + "__merged.csv"
        f = open(new_name, "w")
        for rel_file in relationships[field]:
            with open(rel_file, "r") as ff:
                line = ff.read()
                if line != "" and line not in rel:
                    rel.add(line)
                    f.write(line)
        f.close()

def connect_pivots(from_pivot= "a", to_pivot="b"):
    pivot_file = open("p__" + from_pivot + '__' + to_pivot + '.csv', "w")
    pivot_file.write(":START_ID("+ str(from_pivot) + "-ID),:END_ID("+ str(to_pivot)+"-ID),:TYPE\n")

    obj = set()


    start_file = open("o__" + from_pivot + "__merged.csv", "r")
    for start in start_file:
        start = start.rstrip()
        end_file = open("o__" + to_pivot + "__merged.csv", "r")
        for end in end_file:
            end = end.rstrip()
            if start == end:
                rel = '"' + str(start) + '","' + str(end) + '",RELATES'
                if rel not in obj:
                    obj.add(rel)
        end_file.close()
    start_file.close()

    for o in obj:
        pivot_file.write(o + '\n')

    pivot_file.close()


#get_objects_and_rel_from_csv(csv_file="2007.csv", pivot="FlightNum", whitelist=['FlightNum','TailNum','Origin','Dest'])
#remove_duplicates()
connect_pivots(from_pivot='FlightNum', to_pivot='NewFlightNum')
