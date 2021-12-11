<h1 align="center">Welcome to Knowledge-Graphs</h1>
<p>
</p>

> Creating Knowledge Graphs from Wikipedia data using Spark 

## Author

👤 **Karthik Sahukar , Ankit Kanwar , Sasikiran Karri**

* Github: [@sasikirankarri](https://github.com/sasikirankarri)

## Necessary Configuration files

### bootstrap.sh 

This helps us in running boostrap commands like installing packages etc.. Default execution is included in jupyter-notebook. 

### category.config 

This configuration file will help us in choosing categories from Wikipedia.

### dumplist

List of all Wikipedia dumps to install and run.

### neo4j.config

Creditinals to connect to Neo4j Graph Data base

### requirements

List of Python dependencies

## Instructions

1. Please make sure all configuration files are created. (Please refer to sample config files for your reference.)
2. Open 'Knowledge_Graphs_v1.ipynb' and follow the steps.
3. ALternatively, you can run 'Knowledge_Graphs_v1.py' file
4. Querying Neo4j<br>
    1.1 Please create sandobox from Neo4j using <href>https://neo4j.com/sandbox/</href> and update credentials in neo4j.config<br>
    1.2 Please connect to the sandbox through neo4j browser for querying. <br>
    1.3 Try running cypher queries for getting desire results. <br>
    1.4 Sample query 'MATCH (mv:Movie)-[*]->(pr:Person{name:'Hans Zimmer'}) RETURN mv'<br>
