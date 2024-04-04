# IcebergMetadataRewrite

This project require hive running on local machine,you can start hive with docker image 
in hiveDocker directory. Project will rewrite iceberg metadata, it will accept the table base and update it to all metadata file.  

# Problem 
Suppose you copied an iceberg table(data and metadata directory) from another cluster or move the table 
to another location by copying the directories, this will result in un-readable table.

# Solution
There can be following solutions
1. use custom FileIO which will change base path of all File IO operation 
   here I implemented the custom file IO which read/write files from different path which is not written in metadata,
   instead it's a new base path where you moved your iceberg data and metadata folders 
2. We can rewrite iceberg metadata and register table with updated metadata.(implemented in another project called 
   icebergMetadataRewrite)

# Dependencies
1. Spark (version 3.2)
2. Iceberg-spark-runtime-3.2_2.12 ( version 1.1.0)

# Code walk through 

This project will implement customFileIO for iceberg table which will be used for spark read/write, Please check 
IntegrationTest.java for main flow of the program.
Note: same type of implementation required if you are using hive/presto read 
