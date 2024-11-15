# container_template
Template to containerize a script for swarm. Scripts in any language can be containerized according to this general pattern, but this repo and guide are primarily concerned with containerizing Python.

## Interfacing With Swarm
A script container for swarm must run two commands: spec and read.

### spec 
The spec command prints out JSON representing the name of the puller and the options it accepts from Swarm.

### read 
The read command actually runs your script. When your script runs, it should pull some data and write it to a csv file.
When it is finished running, it prints to the console a string containing 'DONE', then JSON indicating the location of the csv file containing the puller results and a list of headers for that file.

DONE {"status": "ok", "file": "./data.csv", "columns": [{"name": "column_one_name", "type": "VARCHAR"}, {"name": "column_two_name", "type": "BOOLEAN"}}

- A column's name value will appear as the column name in the dataset loaded into swarm.
- The column type should be a valid Postgres type name. The column will be loaded using that type. If the data contains a value that does not match the specified type, the puller will fail.

### troubleshooting
- If you have permission issues, you can run chmod -R 777 . in order to assign all permissions to all users for the files in the directory
