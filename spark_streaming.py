import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

def create_keyspace(session):
    session.execute("""
                    create keyspace if not exists spark_streams
                    with replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
                    """)
    print('keyspace được tạo:.... ')


def create_table(session):
    session.excecute("""
                     create table if not exists spark_streams.created_users(
                         id UUID primary key,
                         first_name text,
                         last_name text,
                         gender text,
                         address text,
                         post_code text,
                         email text,
                         username text,
                         registered_date text,
                         phone text,
                         picture text
                     );
                     
                     """)
    print('Tạo bảng thành công...*-*...')


def insert_data(session,**kwargs):
    print("bảng đang chèn:...")
    id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    post_code = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture= kwargs.get('picture')

    try:
        session.execute("""insert into spark_streams.created_users(id, first_name, last_name, 
                        gender, address, post_code, email, username, registered_date, phone, picture
                        ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", 
                        (id, first_name, last_name, gender, address,
                         post_code, email, username, registered_date,
                         phone, picture))
        logging.info(f"du lieu chen cho user {first_name} {last_name}")
    except Exception as e:
        logging.error(f"khong the chen vao bang loi xay ra tai: {e}")

def create_spark_connect():
    s_conn = None
    try:
        s_conn = SparkSession.builder.appName("Spark Streaming") \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0," "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("spark session được tạo")
    except Exception as e:
        logging.error(f'Lỗi xảy ra tại: {e}')
    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    
    try:
        spark_df = spark_conn.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
    except Exception as e:
        logging.warning(f"kafka dataframe could not created error at {e}")
        
    return spark_df

def create_cassandra_connect():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"kết nối thất bại lỗi xảy ra tại: {e}")
        return None


def create_selection_df_kafka(spark_df):
    schema = StructType([
        StructField('id', StringType(), False),
        StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False),
        StructField('gender', StringType(), False),
        StructField('address', StringType(), False),
        StructField('post_code', StringType(), False),
        StructField('email', StringType(), False),
        StructField('username', StringType(), False),
        StructField('registered_date', StringType(), False),
        StructField('phone', StringType(), False),
        StructField('picture', StringType(), False)   
    ])
    
    sel = spark_df.selectExpr("CAST(value AS STRING)").select(from_json(col('value'), schema).alias('data')).select('data.*')
    print(sel)
    
    return sel
    


if __name__ == "__main__":
    
    spark_conn = create_spark_connect()
    
    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_kafka(spark_df)
        session = create_cassandra_connect()
        
        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
                .option('checkpointLocation', '/tmp/checkpoint') \
                .option('keyspace', 'spark_streams') \
                .option('table', 'created_users') \
                .start()
            )
            streaming_query.awaitTermination()
                