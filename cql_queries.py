# Drop existing tables

song_in_session_table_drop = "DROP TABLE IF EXISTS song_in_session"
user_songs_table_drop = "DROP TABLE IF EXISTS user_songs"
song_listeners_table_drop = "DROP TABLE IF EXISTS song_listeners"


# Create tables

song_in_session_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_in_session (
        sessionId int,
        itemInSession int,
        artist text,
        song text,
        length float,
        PRIMARY KEY (sessionId, itemInSession))
""")

user_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_songs (
        userId int,
        sessionId int,
        itemInSession int,
        song text,
        artist text,
        firstName text,
        lastName text,
        PRIMARY KEY (userId, sessionId, itemInSession))
""")

song_listeners_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_listeners (
        song text,
        userId int,
        firstName text,
        lastName text,
        PRIMARY KEY (song, userId))
""")


# Insert records

song_in_session_table_insert = ("""
    INSERT INTO song_in_session (
        sessionId,
        itemInSession,
        artist,
        song,
        length)
    VALUES (%s, %s, %s, %s, %s)
""")

user_songs_table_insert = ("""
    INSERT INTO user_songs (
        userId,
        sessionId,
        itemInSession,
        song,
        artist,
        firstName,
        lastName)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

song_listeners_table_insert = ("""
    INSERT INTO song_listeners (
        song,
        userId,
        firstName,
        lastName)
    VALUES (%s, %s, %s, %s)
""")


# Select statements

song_in_session_table_select = """
    SELECT artist, song, length FROM song_in_session
    WHERE sessionId = 338 AND itemInSession = 4
"""

user_songs_table_select = """
    SELECT iteminsession, artist, song, firstname, lastname FROM user_songs
    WHERE userId = 10 AND sessionId = 182
"""

song_listeners_table_select = """
    SELECT firstName, lastName FROM song_listeners
    WHERE song = 'All Hands Against His Own'
"""


# Query lists
create_table_queries = [
    song_in_session_table_create,
    user_songs_table_create,
    song_listeners_table_create
]

drop_table_queries = [
    song_in_session_table_drop,
    user_songs_table_drop,
    song_listeners_table_drop
]
