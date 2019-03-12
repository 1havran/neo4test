import csv


def read_csv(csv_file, pivot="", delim=","):
    line_reads = 0
    header = []
    pivot_id = 0
    object_files = []
    relationships_files = []
    objects = []
    relationships = []

    with open(csv_file) as csvfile:
        data = csv.reader(csvfile, delimiter=delim)
        for row in data:
            line_reads += 1
            if line_reads == 1:
                header = row
                for i in range(len(header)):

                    name = "o__" + str(row[i]) + "__" + csv_file
                    print(name)
                    new_file = open(name, "w")
                    new_file.write(row[i] + ":value(" + str(row[i]) + "-ID)\n")
                    object_files.append(new_file)
                    objects.append(set())

                    if row[i] == pivot:
                        pivot_id = i

                    rel_name = "r__" + pivot + "__" + str(row[i]) + "__" + csv_file
                    rel_file = open(rel_name, "w")
                    rel_file.write(":START_ID("+ str(pivot) + "-ID),:END_ID("+ str(row[i])+"-ID),:TYPE\n")
                    relationships.append(set())
                    relationships_files.append(rel_file)

            else:
                for i in range(len(row)):
                    if row[i] not in objects[i]:
                        objects[i].add(row[i])
                    if i != pivot_id:
                        rel = '"' + str(row[pivot_id]) + '","' + str(row[i]) + '",HAS'
                        if rel not in relationships[i]:
                           relationships[i].add(rel)

    for i in range(len(header)):
        for element in objects[i]:
            object_files[i].write(element + "\n")
        object_files[i].close()

    for i in range(len(relationships)):
        for element in relationships[i]:
            relationships_files[i].write(element + "\n")
        relationships_files[i].close()



#read_csv(csv_file="template.csv", pivot="FlightNum")
read_csv(csv_file="2007.csv", pivot="FlightNum")
