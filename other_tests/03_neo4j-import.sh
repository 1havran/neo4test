#!/bin/bash

neo4j-admin import --database=ipv4.db --nodes file-0_4.csv --nodes file-112_148.csv --nodes file-148_184.csv --nodes file-184_220.csv --nodes file-220_256.csv --nodes subnets-30000.csv --relationships relationships-30000.csv
