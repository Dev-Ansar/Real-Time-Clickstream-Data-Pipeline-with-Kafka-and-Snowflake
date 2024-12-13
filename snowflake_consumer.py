from kafka import KafkaConsumer
import json
import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Kafka and Snowflake configuration
bootstrap_servers = 'localhost:9092'
topics = ['videoplayback', 'searchevents', 'likedislikeevents', 'navigationevents']

# Snowflake connection details
conn = snowflake.connector.connect(
    user=os.getenv("user"),
    password=os.getenv("password"),
    account=os.getenv("account"),
    warehouse=os.getenv("warehouse"),
    database=os.getenv("database"),
    schema=os.getenv("schema")
)

cur = conn.cursor()

# Kafka Consumer
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=bootstrap_servers,
    group_id='clickstream-consumer-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

import json

def insert_into_snowflake(table_name, event_data):
    try:
        # Convert the event_data to a string (instead of using PARSE_JSON)
        query = f"""
            INSERT INTO events.{table_name} (event_data, inserted_time)
            VALUES ('{json.dumps(event_data)}', CURRENT_TIMESTAMP)
        """
        cur.execute(query)
        print(f"Inserted data into {table_name}: {event_data}")
    except Exception as e:
        print(f"Error inserting data into Snowflake: {e}")


# Process messages from Kafka
try:
    print("Consumer is listening for messages...")
    for message in consumer:
        topic = message.topic
        data = message.value
        print(f"Consumed message from topic '{topic}': {data}")
        
        # Route data to the appropriate Snowflake table
        if topic == 'videoplayback':
            insert_into_snowflake('videoplayback', data)
        elif topic == 'searchevents':
            insert_into_snowflake('searchevents', data)
        elif topic == 'likedislikeevents':
            insert_into_snowflake('likedislikeevents', data)
        elif topic == 'navigationevents':
            insert_into_snowflake('navigationevents', data)
except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    consumer.close()
    cur.close()
    conn.close()
