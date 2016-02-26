## Neo4j adnvantures with py2neo
This repository contains few performance testing code, which has helped me to better understand Neo4j by reaching limits.


----------

###Description 
I work with alexa.com top 1 million site list to during all tests(http://s3.amazonaws.com/alexa-static/top-1m.csv.zip). There is an option to download and work with different data, but there may be issues with data structuring.

After parsing the file, the information is broken down into 5 different segments which are used as properties. These can be used as base for creating edges or test run queries.

###Dependencies
Community edition of Neo4j available here [Neo4j Download](http://neo4j.com/download/) 
Python 2,7 available here [Python download](https://www.python.org/downloads/)

Libraries required for python

    pip install py2neo
    pip install psutil

NOTE: Please note that there is a "delete all" query which will wipe everything in the database, I suggest use an empty database!

###Execution
Use the basic_runner.py to execute

