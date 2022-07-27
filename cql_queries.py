# Drop existing tables

song_in_session_table_drop = "DROP TABLE IF EXISTS "
user_songs_table_drop = "DROP TABLE IF EXISTS  "
song_listeners_table_drop = "DROP TABLE IF EXISTS "


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
