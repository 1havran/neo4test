#!/bin/bash

neo4j-admin import --database=subnets.db --nodes subnets-300000.csv --nodes ipaddresses-300000.csv --relationships relationships-300000.csv
