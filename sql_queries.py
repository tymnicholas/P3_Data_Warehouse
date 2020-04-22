import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN= config.get('IAM_ROLE', 'ARN')
LOG_DATA= config.get('S3', 'LOG_DATA')
SONG_DATA= config.get('S3', 'SONG_DATA')
LOG_JSONPATH= config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
artist VARCHAR,
auth VARCHAR, 
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
sessionId INTEGER,
song VARCHAR,
status INTEGER,
ts TIMESTAMP,
userAgent VARCHAR,
userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
song_id VARCHAR,
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
title VARCHAR,
duration FLOAT,
year INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY SORTKEY,
start_time TIMESTAMP NOT NULL,
user_id INTEGER NOT NULL,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id INTEGER NOT NULL,
location VARCHAR,
user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id INTEGER PRIMARY KEY SORTKEY,
first_name VARCHAR NOT NULL,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id VARCHAR PRIMARY KEY SORTKEY,
title VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
year INTEGER,
duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id VARCHAR PRIMARY KEY SORTKEY,
name VARCHAR NOT NULL,
location VARCHAR,
latitude FLOAT,
longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS times(
start_time TIMESTAMP PRIMARY KEY SORTKEY,
hour INTEGER NOT NULL,
day INTEGER NOT NULL,
week INTEGER NOT NULL,
month INTEGER, NOT NULL
year INTEGER NOT NULL,
weekday INTEGER NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    timeformat 'epochmillisecs'
    json {};
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    json 'auto';
""").format(SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(
start_time,
user_id,
level,
song_id,
artist_id,
session_id,
location,
user_agent)
SELECT DISTINCT (se.ts),
se.userId as user_id,
se.level,
s.song_id,
s.artist_id,
se.sessionId as session_id,
se.location,
se.userAgent as user_agent

FROM staging_events se
JOIN staging_songs s on se.song = s.title
AND se.artist = s.artist_name
""")

user_table_insert = ("""
INSERT INTO users(user_id,
first_name,
last_name,
gender,
level)
SELECT DISTINCT
se.userId as user_id,
se.firstName as first_name,
se.lastName as last_name,
se.gender,
se.level

FROM staging_events se
WHERE se.userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs(song_id,
title,
artist_id,
year,
duration)
SELECT DISTINCT 
s.song_id,
s.title,
s.artist_id,
s.year,
s.duration

FROM staging_songs s
WHERE s.artist_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id,
name,
location,
latitude,
longitude)
SELECT DISTINCT
s.artist_id,
s.artist_name as name,
s.artist_location as location,
s.artist_latitude as latitude,
s.artist_longitude as longitude

FROM staging_songs s
WHERE s.artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO times(start_time,
hour,
day,
week,
month,
year,
weekday)
SELECT DISTINCT ts,
EXTRACT(hour from ts),
EXTRACT(day from ts),
EXTRACT(week from ts),
EXTRACT(month from ts),
EXTRACT(year from ts),
EXTRACT(weekday from ts)

FROM staging_events se
WHERE se.ts IS NOT NULL

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
