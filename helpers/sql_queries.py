import configparser


# CONFIG

""" The config variable below creates a ConfigParser() class
    which allows python to read the contents of the dwh.cfg file """

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

""" The SQL queries in this section are created to drop any of the tables
    I am looking to create to ensure they don't already exist - this
    just ensures I am not adding more data to tables that already exist
    which might cause an issue further doen the line. These queries are
    then stored in separate variables ready for use later on """

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

""" Following on from the above process, the fact & dimension
    tables are then created providing that they do not already
    exist. Column names for the tables are provided along with
    the type for the data that is to be included in each column. 
    Similarly, like the above function, these are stored in separate
    variables ready for use later. """
    

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
 artist VARCHAR(50),
 auth VARCHAR(50),
 firstName VARCHAR(50),
 gender VARCHAR(1),
 itemInSession INT,
 lastName VARCHAR(50),
 length DOUBLE PRECISION,
 level VARCHAR(50),
 location VARCHAR(100),
 method VARCHAR(10),
 page VARCHAR(20),
 registration INT,
 sessionId INT,
 song VARCHAR(200),
 status INT,
 ts TIMESTAMP,
 userAgent VARCHAR(200),
 userId INT
);

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs

(
 num_songs INT,
 artist_id VARCHAR(50),
 artist_latitude DOUBLE PRECISION,
 artist_longitude DOUBLE PRECISION,
 artist_location VARCHAR(150),
 artist_name VARCHAR(100),
 song_id INT,
 title VARCHAR(150),
 duration DOUBLE PRECISION,
 year INT
);

""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays

(
 songplay_id INT IDENTITY(0,1) PRIMARY KEY,
 start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
 user_id INT NOT NULL REFERENCES users(user_id),
 level VARCHAR NOT NULL,
 song_id INT NOT NULL REFERENCES songs(song_id),
 artist_id INT NOT NULL REFERENCES artists(artist_id),
 session_id INT NOT NULL,
 location VARCHAR NOT NULL,
 user_agent VARCHAR NOT NULL
);

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users

(
 user_id INT NOT NULL PRIMARK KEY,
 first_name VARCHAR(50) NOT NULL,
 last_name VARCHAR(50) NOT NULL,
 gender VARCHAR(1) NOT NULL,
 level VARCHAR(50) NOT NULL
);

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs

(
 song_id INT NOT NULL PRIMARY KEY,
 title VARCHAR(150) NOT NULL,
 artist_id VARCHAR(50) NOT NULL,
 year INT NOT NULL,
 duration DOUBLE PRECISION NOT NULL
);

""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists

(
 artist_id VARCHAR(50) NOT NULL PRIMARY KEY,
 name VARCHAR(100) NOT NULL,
 location VARCHAR(150) NOT NULL,
 latitude DOUBLE PRECISION NOT NULL,
 longitude DOUBLE PRECISION NOT NULL
);

""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time

(
 start_time TIMESTAMP NOT NULL PRIMARY KEY,
 hour INT NOT NULL,
 day INT NOT NULL,
 week INT NOT NULL,
 month INT NOT NULL,
 year INT NOT NULL,
 weekday VARCHAR(15) NOT NULL
);

""")

# STAGING TABLES

""" In this section, the data contained in S3 for Sparkify is
    copied from S3 using the COPY command. The filepath for this
    data, the IAM_ROLE, and IAM_ROLE_ARN to access the postgres
    database in AWS are loaded into each placeholder by using the
    '.format' command - This replaces the values with those located
    in the dwh.cfg file & is then stored in the staging_events_copy
    and staging_songs_copy variables. """

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role '{}'
    region 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    JSON {};
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]
            ["IAM_ROLE_ARN"], config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role '{}'
    region 'us-west-2'
    JSON 'auto';
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]
            ["IAM_ROLE_ARN"])

# FINAL TABLES

""" This last section inserts all the data from each
    of the datasets either directly into each table or
    by joining the two datasets together. As the fact table
    contains information from both datasets, the two are joined
    together to access all the relevant data, however, the
    dimensions do not require this as data can be pulled
    directly from the source without the need for joining.
    As before, these are stored into variables which will be
    later on when running the create_tables.py file & the
    etl.py file. """

songplay_table_insert = ("""INSERT INTO songplays
(songplay_id, start_time, user_id, level, song_id,
artist_id, session_id, location, user_agent)
SELECT e.sessionId, e.itemInSession,
CONCAT(e.sessionId, ‘-‘, e.itemInSession) as songplay_id,
e.ts as start_time, e.userId as user_id, e.level, s.song_id,
s.artist_id, e.sessionId as session_id, e.location, e.userAgent as user_agent
FROM staging_events e
JOIN staging_songs s
ON s.artist_name=e.artist AND s.title=e.song
""")

user_table_insert = ("""INSERT INTO users
(user_id, first_name, last_name, gender,
level)
SELECT userId as user_id, firstName as first_name,
lastName as last_name, gender, level
FROM staging_events
""")

song_table_insert = ("""INSERT INTO songs
(song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id,
year, duration
FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists
(artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name as name,
artist_location as location, artist_latitude as latitude,
artist_longitude as longitude
FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time
(start_time, hour, day, week, month, year,
weekday)
SELECT ts AS start_time,
EXTRACT (hr from ts) AS hour,
EXTRACT (d from ts) AS day,
EXTRACT (w from ts) AS week,
EXTRACT (mon from ts) AS month,
EXTRACT (weekday from ts) AS weekday
FROM (
              SELECT DISTINCT ts
              FROM staging_events
)
WHERE ts NOT IN (SELECT DISTINCT start_time FROM time)
AND ts IS NOT NULL
""")

# QUERY LISTS

""" Below, each separate variable created above is added to a
    list of create, drop & copy queries for greater efficiency. """

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
