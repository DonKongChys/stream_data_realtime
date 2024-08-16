from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def check_cassandra_connection():
    try:
        # Authentication provider with username and password
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        
        # Connecting to the Cassandra cluster
        cluster = Cluster(['localhost'], auth_provider=auth_provider)
        # cluster = Cluster(['localhost'])
        
        session = cluster.connect()
        
        print("Cassandra connection created successfully!")

        # Example query
        # rows = session.execute("SELECT * FROM spark_streams.created_users")
        # for row in rows:
        #     print(row)
        print(session)
        print("shutdown session")
        session.shutdown()
    except Exception as e:
        print(f"Couldn't create the Cassandra connection due to exception: {e}")
        return None

if __name__ == "__main__":
    check_cassandra_connection()