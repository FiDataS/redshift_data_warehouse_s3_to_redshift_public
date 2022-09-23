import configparser


# CONFIG
# Import Configparser to read the config in dwh.cfg
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
# SQL Statements for dropping the tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# SQL Statements for creating the tables
staging_events_table_create= ("""CREATE TABLE staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession BIGINT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId BIGINT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId BIGINT
    );
    """)

staging_songs_table_create = ("""CREATE TABLE staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
    );
    """)

songplay_table_create = ("""CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL,
    start_time BIGINT,
    user_id BIGINT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id BIGINT,
    location VARCHAR,
    user_agent VARCHAR    
    );
""")

user_table_create = ("""CREATE TABLE users (
    user_id BIGINT PRIMARY KEY NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
    );
""")

song_table_create = ("""CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY NOT NULL,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration FLOAT
    );
""")

artist_table_create = ("""CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY NOT NULL,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
    );
""")

time_table_create = ("""CREATE TABLE time (
    start_time TIMESTAMP NOT NULL SORTKEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
    );
""")

# STAGING TABLES
# COPY command for loading the data for events and songs from S3 bucket
# Documentation here: https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html#r_COPY_command_examples-copy-from-json


###### Clarify if one needs the 'us-west-2' ??
staging_events_copy = ("""COPY staging_events FROM {data_bucket}
    IAM_ROLE {role_arn}
    region 'us-west-2'
    FORMAT AS JSON {log_json_path};
    """).format(data_bucket=config['S3']['LOG_DATA'],role_arn=config['IAM_ROLE']['ARN'],log_json_path=config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""COPY staging_songs FROM {data_bucket}
    IAM_ROLE {role_arn}
    region 'us-west-2' 
    JSON 'auto';
    """).format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES
# Creating the final tables with SQL commands from the staging tables

# FACT TABLE
# Table for all NextSong events in the data
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT SE.ts, SE.userId, SE.level, SS.song_id, SS.artist_id, SE.sessionId, SE.location, SE.useragent
    FROM staging_events SE LEFT JOIN staging_songs SS ON SE.song = SS.title AND SE.artist = SS.artist_name
    WHERE SE.page = 'NextSong' AND SE.userId IS NOT NULL
    """)

# DIMENSION TABLES
# Table for all distinct users in the app
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId IS NOT NULL
    """)

# Table for all songs listed in the song data
song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    """)

# Table for all artists listed in the song_data
artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    """)

# Table for all timestamps in songplays separated in columns for hour, day, week, etc.
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    WITH tmp AS (SELECT (TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second') AS ts_TS FROM songplays)
    SELECT ts_TS, DATE_PART(hrs, ts_TS), DATE_PART(dayofyear, ts_TS), DATE_PART(w, ts_TS), DATE_PART(mon, ts_TS), DATE_PART(y, ts_TS), DATE_PART(dow, ts_TS)
    FROM tmp
    """)


# QUERY LISTS
# Summarize queries to be executed in the following python scripts

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
