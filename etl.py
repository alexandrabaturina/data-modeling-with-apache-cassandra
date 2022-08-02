import os
import glob
import csv
import sys
from cassandra.cluster import Cluster
from cql_queries import *
import time
from time import sleep

PROGRESS_BAR_LENGTH = 30
QUERY_1 = """
Find the artist, song title and song's length in the music app history
that was heard during sessionId = 338, and itemInSession = 4."""
QUERY_2 = """
Find only the following: name of artist, song (sorted by itemInSession) and
user (first and last name) for userid = 10, sessionid = 182."""
QUERY_3 = """
Find every user name (first and last) in my music app history who listened
to the song 'All Hands Against His Own'."""


def display_bar(percent):
    """
    Function to display progress bar.

    Args:
        percent(float): Current percent complete
    Returns:
        no value
    """
    sys.stdout.write('\r')
    sys.stdout.write("Completed: [{:{}}] {:>3}%"
        .format('='*int(percent/(100.0/PROGRESS_BAR_LENGTH)),
            PROGRESS_BAR_LENGTH, int(percent)))
    sys.stdout.flush()
    time.sleep(0.002)


def prepare_data():
    """
    Function to create event_datafile_full.csv file from files in /event_data.

    Args:
        no value
    Returns:
        no value
    """
    # Get current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        # Join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))

    # Initiate an empty list of rows that will be generated from each file
    full_data_rows_list = []

    for f in file_path_list:
        # Read csv file
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            # Create a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

            # Extract each data row one by one and append it
            for line in csvreader:
                full_data_rows_list.append(line)

    # Get total number of rows
    print('Total number of rows:', len(full_data_rows_list))

    # Create a smaller event_datafile_full csv file that will be used to
    # insert data into the Apache Cassandra tables
    csv.register_dialect(
        'myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open(
        'event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow([
            'artist',
            'firstName',
            'gender',
            'itemInSession',
            'lastName',
            'length',
            'level',
            'location',
            'sessionId',
            'song',
            'userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((
                row[0],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[12],
                row[13],
                row[16]))


def process_data(session):
    """
    Function to insert data in tables and to run select queries.

    Args:
        session(session): session connection a Cassandra cluster
    Returns:
        no value
    """
    file = 'event_datafile_new.csv'

    line_counter = 0

    with open(file, 'r', encoding = 'utf8') as f:
        total_lines = sum(1 for line in f)
    f.close()

    # Insert data in song_in_session table
    print('Inserting data in song_in_session table...')
    line_counter = 0

    with open(file, 'r', encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header

        for line in csvreader:
            try:
                session.execute(
                    song_in_session_table_insert, (
                        int(line[8]),
                        int(line[3]),
                        line[0],
                        line[9],
                        float(line[5])))
            except Exception as e:
                print(e)

            line_counter += 1
            display_bar(100.0 * line_counter/(total_lines - 1))
    print("\n")
    f.close()

    # Insert data in user_songs table
    print('Inserting data in user_songs table...')
    line_counter = 0

    with open(file, 'r', encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header

        for line in csvreader:
            try:
                session.execute(
                    user_songs_table_insert, (
                        int(line[10]),
                        int(line[8]),
                        int(line[3]),
                        line[9],
                        line[0],
                        line[1],
                        line[4]))
            except Exception as e:
                print(e)

            line_counter += 1
            display_bar(100.0 * line_counter/(total_lines - 1))
    print("\n")
    f.close()

    # Insert data in song_listeners table
    print('Inserting data in song_listeners table...')
    line_counter = 0

    with open(file, 'r', encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header

        for line in csvreader:
            try:
                session.execute(
                    song_listeners_table_insert, (
                        line[9],
                        int(line[10]),
                        line[1],
                        line[4]))
            except Exception as e:
                print(e)

            line_counter += 1
            display_bar(100.0 * line_counter/(total_lines - 1))
    print("\n")
    f.close()

    # Perform select queries
    print('Query 1:', QUERY_1)
    try:
        rows = session.execute(song_in_session_table_select)
    except Exception as e:
        print(e)

    if rows:
        for row in rows:
            print(row.artist, row.song, row.length)
    else:
        print('Nothing found.')
    print('\n')

    print('Query 2:', QUERY_2)

    try:
        rows = session.execute(user_songs_table_select)
    except Exception as e:
        print(e)

    if rows:
        for row in rows:
            print(row.iteminsession, row.artist, row.firstname, row.lastname)
    else:
        print('Nothing found.')
    print('\n')

    print('Query 3:', QUERY_3)
    try:
        rows = session.execute(song_listeners_table_select)
    except Exception as e:
        print(e)

    if rows:
        for row in rows:
            print(row.firstname, row.lastname)
    else:
        print('Nothing found.')


def main():
    prepare_data()

    cluster = Cluster(['127.0.0.1'])
    # Create a session to establish connection and begin executing queries
    session = cluster.connect()
    session.set_keyspace('music_app_data')

    process_data(session)

    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
