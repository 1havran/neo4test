#!/bin/bash

neo4j-admin import --database=subnets.db --nodes subnets.csv --nodes ipaddresses.csv --relationships relationships.csv
