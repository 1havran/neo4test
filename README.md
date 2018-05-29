	$ neo4j-admin import --database=ipv4.db --nodes "file-0_4.csv,file-112_148.csv,file-148_184.csv,file-184_220.csv,file-220_256.csv,subnets-30000.csv" --relationships relationships-30000.csv                                                                                                                                              
	Neo4j version: 3.4.0
	Importing the contents of these files into /var/lib/neo4j/data/databases/ipv4.db:
	Nodes:
	  /home/1havran/git/neo4test/file-0_4.csv
	  /home/1havran/git/neo4test/file-112_148.csv
	  /home/1havran/git/neo4test/file-148_184.csv
	  /home/1havran/git/neo4test/file-184_220.csv
	  /home/1havran/git/neo4test/file-220_256.csv
	  /home/1havran/git/neo4test/subnets-30000.csv
	Relationships:
	  /home/1havran/git/neo4test/relationships-30000.csv
	
	Available resources:
	  Total machine memory: 31.27 GB
	  Free machine memory: 254.09 MB
	  Max heap memory : 6.95 GB
	  Processors: 8
	  Configured max memory: 21.89 GB
	  High-IO: false
	
	WARNING: 285.23 MB memory may not be sufficient to complete this import. Suggested memory distribution is:
	heap size: 1.13 GB
	minimum free and available memory excluding heap size: 36.44 GBImport starting 2018-05-29 21:54:17.190+0200
	  Estimated number of nodes: 2.66 G
	  Estimated number of node properties: 5.32 G
	  Estimated number of relationships: 4.32 M
	  Estimated number of relationship properties: 0.00 
	  Estimated disk space usage: 138.92 GB
	  Estimated required memory usage: 36.44 GB
	
	InteractiveReporterInteractions command list (end with ENTER):
	  c: Print more detailed information about current stage
	  i: Print more detailed information
	
	(1/4) Node import 2018-05-29 21:54:17.206+0200
	  Estimated number of nodes: 2.66 G
	  Estimated disk space usage: 138.79 GB
	  Estimated required memory usage: 36.44 GB
	
