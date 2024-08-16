import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy

import logging


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        postcode TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        phone TEXT,
        registered_date TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")
    
def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                postcode, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')
    
def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "users_created") \
                .option("startingOffsets", "earliest") \
                .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_cassandra_connection():
    try:
        # Authentication provider with username and password
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        
        # Connecting to the Cassandra cluster with load-balancing policy and protocol version
        # Connecting to the Cassandra cluster
        cluster = Cluster(['localhost'], auth_provider=auth_provider)
        session = cluster.connect()

        print("Created sesssion successfully!!!")
        return session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None
    
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    # print(sel)

    return sel

if __name__ == "__main__":

    spark_conn = create_spark_connection()

    spark_df = connect_to_kafka(spark_conn)

    # # Chuyển đổi dữ liệu
    selection_df = create_selection_df_from_kafka(spark_df)
    
    # Cast columns to appropriate data types if needed
    selection_df = selection_df.select(
        selection_df["id"].cast(StringType()),
        selection_df["first_name"].cast(StringType()),
        selection_df["last_name"].cast(StringType()),
        selection_df["gender"].cast(StringType()),
        selection_df["address"].cast(StringType()),
        selection_df["postcode"].cast(StringType()),
        selection_df["email"].cast(StringType()),
        selection_df["username"].cast(StringType()),
        selection_df["dob"].cast(StringType()),
        selection_df["phone"].cast(StringType()),
        selection_df["registered_date"].cast(StringType()),
        selection_df["picture"].cast(StringType())
    )



    try:

        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)
            logging.info("Streaming is being started...")

            # Log schema and sample data
            selection_df.printSchema()
                
            # streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra") 
            #                     .option("checkpointLocation", '/tmp/checkpoint')
            #                     .options(table="created_users", keyspace="spark_streams") 
            #                     .outputMode("append") 
            #                     .start())

            streaming_query = (selection_df.writeStream
                        .format("org.apache.spark.sql.cassandra")
                        .option("checkpointLocation", '/tmp/checkpoint')
                        .option("table", "created_users")
                        .option("keyspace", "spark_streams")
                        .outputMode("append")
                        .start())
            streaming_query.awaitTermination()
    except Exception as e:
        if session:
            print("shutdown session....")
            session.shutdown()
        print(e)



    # # Hiển thị kết quả ra console
    # console_query = selection_df.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()

    # # # Đợi cho đến khi kết thúc
    # console_query.awaitTermination()