#!/bin/bash

neo4j-admin import --database=ipv4.db --nodes "file-0_4.csv,file-112_148.csv,file-148_184.csv,file-184_220.csv,file-220_256.csv,subnets-30000.csv" --relationships relationships-30000.csv
