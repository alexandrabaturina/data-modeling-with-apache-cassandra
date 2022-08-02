import cassandra
from cql_queries import create_table_queries, drop_table_queries

def create_keyspace():
    # Make a connection to a Cassandra instance on local machine
    from cassandra.cluster import Cluster
    cluster = Cluster(['127.0.0.1'])

    # Create a session to establish connection and begin executing queries
    session = cluster.connect()

    # Create a keyspace
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS music_app_data
            WITH REPLICATION = {
                'class' : 'SimpleStrategy',
                'replication_factor' : 1
            }
        """
        )
    except Exception as e:
        print(e)

    # Set keyspace to newly created keyspace
    session.set_keyspace('music_app_data')

    return session

def drop_tables(session):
    for query in drop_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)
    print('Tables are dropped.')

def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)
    print('Tables are created.')

def main():
    session = create_keyspace()
    drop_tables(session)
    create_tables(session)

if __name__ == "__main__":
    main()
