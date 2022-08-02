# Data Engineering ND #2: Data Modeling With Apache Cassandra
## Overview
Data Modeling With Apache Cassandra is the second project of Udacity [Data Engineering Nanodegree](https://d20vrrgs8k4bvw.cloudfront.net/documents/en-US/Data+Engineering+Nanodegree+Program+Syllabus.pdf). It requires to create an [Apache Cassandra](https://cassandra.apache.org/_/index.html) database for a music streaming app and write an ETL pipeline that transfers data from directory of CSV files into this database using Python and [CQL](https://cassandra.apache.org/doc/latest/cassandra/cql/index.html).

The project has the following goals:
 * Model a NoSQL database with [Apache Cassandra](https://cassandra.apache.org/_/index.html)
 * Perform insert and select queries using [CQL](https://cassandra.apache.org/doc/latest/cassandra/cql/index.html)
 * Create ETL pipeline with Python
## Repo Contents
The repo contains the ```event_data``` directory of csv files on user activity on the app, as well as the following files:
 * ```cql_queries.py```: contains all CQL queries.
 * ```create_tables.py```: drops existing tables and creates new ones.
 * ```data_modeling_with_cassandra.ipynb```: reads and processes csv files from ```event_data```, loads the data into tables, and runs ```SELECT``` queries.
 * ```etl.py```:  creates ```event_datafile_full.csv``` file from csv files in ```event_data```, loads the data from this file into tables, and runs ```SELECT``` queries.
