# Data Engineering ND #2: Data Modeling With Apache Cassandra
## Overview
Data Modeling With Apache Cassandra is the second project of Udacity [Data Engineering Nanodegree](https://d20vrrgs8k4bvw.cloudfront.net/documents/en-US/Data+Engineering+Nanodegree+Program+Syllabus.pdf). It requires to create an [Apache Cassandra](https://cassandra.apache.org/_/index.html) database for a music streaming app and write an ETL pipeline that transfers data from directory of CSV files into this database using Python and [CQL](https://cassandra.apache.org/doc/latest/cassandra/cql/index.html).

The project has the following goals:
 * Model a NoSQL database with [Apache Cassandra](https://cassandra.apache.org/_/index.html)
 * Perform insert and select queries using [CQL](https://cassandra.apache.org/doc/latest/cassandra/cql/index.html)
 * Create ETL pipeline with Python
## Repo Contents
The repo contains the ```event_data``` directory of CSV files on user activity on the app, as well as the following files:
 * ```cql_queries.py```: contains all CQL queries.
 * ```create_tables.py```: drops existing tables and creates new ones.
 * ```data_modeling_with_cassandra.ipynb```: reads and processes CSV files from ```event_data```, loads the data into tables, and runs ```SELECT``` queries.
 * ```etl.py```:  creates ```event_datafile_full.csv``` file from CSV files in ```event_data```, loads the data from this file into tables, and runs ```SELECT``` queries.
## Apache Cassandra Database
### Database Purpose
A startup called *Sparkify* wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 

The **music_app_data** database is designed to answer the following questions:
 1. Find the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4.
 2. Find only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182.
 3.  Find every user name (first and last) in my music app history who listened to the song *'All Hands Against His Own'*.
 
### Data Description
The ```event_data``` directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```
Each CSV file in ```event_data``` directory contains data within the following columns:
  * artist
  * auth
  * firstName
  * gender
  * itemInSession
  * lastName
  * length
  * level
  * location
  * method
  * page
  * registration
  * sessionId
  * song
  * status
  * ts
  * userId

The ```event_datafile_new.csv``` file that will be used for Apache Cassandra tables has the following columns:
  * artist
  * firstName
  * gender
  * itemInSession
  * lastName
  * length
  * level
  * location
  * sessionId
  * song
  * userId

Below is an example of how the data in ```event_datafile_new.csv``` looks like.
![image](https://user-images.githubusercontent.com/53233637/182494081-365123f5-2d5b-4a8a-9b44-185708f9cf3b.png)
## Database Design
The **music_app_data** database is designed so that each table answers one question. It contains the following tables.
### ```song_in_session```
The ```song_in_session``` table has the following fields:
  * sessionId: int
  * itemInSession: int
  * artist: text
  * song: text
  * length: float
  * **PRIMARY KEY (sessionId, itemInSession)**
### ```user_songs```
The ```user_songs``` table has the following fields:
 * userId: int
 * sessionId: int
 * itemInSession: int
 * song: text
 * artist: text
 * firstName: text
 * lastName: text
 * **PRIMARY KEY (userId, sessionId, itemInSession))**
### ```song_listeners```
The ```song_listeners``` table has the following fields:
 * song: text
 * userId: int
 * firstName: text
 * lastName: text
 * **PRIMARY KEY (song, userId))**
## Getting Started
To run ETL pipeline locally,
1. Clone this repo.
2. ```cd``` into project directory.
3. Run ```create_tables.py``` to reset tables:
```
root@8fa691392031:/home/workspace# python create_tables.py
Tables are dropped.
Tables are created.
```
> Remember to run ```create_tables.py``` every time before running ```etl.py``` to reset tables.
4. Run ```etl.py```. Once tables are creates, query results are displayed.
```
root@8fa691392031:/home/workspace# python etl.py
Total number of rows: 8056
Inserting data in song_in_session table...
Completed: [==============================] 100%

Inserting data in user_songs table...
Completed: [==============================] 100%

Inserting data in song_listeners table...
Completed: [==============================] 100%

Query 1:
Find the artist, song title and song's length in the music app history
that was heard during sessionId = 338, and itemInSession = 4.
Faithless Music Matters (Mark Knight Dub) 495.30731201171875


Query 2:
Find only the following: name of artist, song (sorted by itemInSession) and
user (first and last name) for userid = 10, sessionid = 182.
0 Down To The Bone Sylvie Cruz
1 Three Drives Sylvie Cruz
2 Sebastien Tellier Sylvie Cruz
3 Lonnie Gordon Sylvie Cruz


Query 3:
Find every user name (first and last) in my music app history who listened
to the song 'All Hands Against His Own'.
Jacqueline Lynch
Tegan Levine
Sara Johnson
```

## Authors
Alexandra Baturina

## Acknowledgments
Implementation of python progress bar is based on [this solution](https://gist.github.com/sibosutd/c1d9ef01d38630750a1d1fe05c367eb8).
