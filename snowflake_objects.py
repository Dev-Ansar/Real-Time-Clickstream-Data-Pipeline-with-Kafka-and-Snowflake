import os
import snowflake.connector
from dotenv import load_dotenv


# Load environment variables from the .env file
load_dotenv()


user = os.getenv("user")
print(user)


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

# Create database, schema, and tables
try:
    # Create database and schema
    cur.execute("CREATE DATABASE IF NOT EXISTS movie_clickstream;")
    cur.execute("USE DATABASE movie_clickstream;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS events;")
    cur.execute("USE SCHEMA events;")
    
    # Create tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS videoplayback (
            event_data VARCHAR,
            inserted_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS searchevents (
            event_data VARCHAR,
            inserted_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS likedislikeevents (
            event_data VARCHAR,
            inserted_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS navigationevents (
            event_data VARCHAR,
            inserted_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    print("Database and tables created successfully.")
finally:
    cur.close()
    conn.close()
