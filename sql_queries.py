import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
## STAGING TABLES ##
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
(event_id integer IDENTITY(0,1) PRIMARY KEY,
artist text,
auth text,
first_name text,
gender text,
item_in_session text,
last_name text,
length text,
level text,
location text,
method text,
page text,
registration text,
session_id text,
song text,
status text,
start_time text,
user_agent text,
user_id text
);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(num_songs text,
artist_id text,
artist_latitude double precision,
artist_longitude double precision,
artist_location text,
artist_name text,
song_id text PRIMARY KEY,
title text,
duration double precision,
year integer
);""")

## OLAP TABLES ##
user_table_create = ("""CREATE TABLE IF NOT EXISTS users
(user_id text PRIMARY KEY,
first_name text,
last_name text,
gender text,
level text)DISTSTYLE all;""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
(artist_id text PRIMARY KEY,
artist_name text,
artist_location text,
artist_latitude double precision,
artist_longitude double precision)DISTSTYLE all;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
(song_id text PRIMARY KEY,
title text,
artist_id text NOT NULL REFERENCES artists(artist_id) sortkey distkey,
year integer,
duration double precision);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
(start_time timestamp PRIMARY KEY,
hour integer,
day integer,
week integer,
month integer,
year integer,
weekday text)DISTSTYLE all;""")


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
(songplay_id integer IDENTITY(0,1) PRIMARY KEY,
start_time timestamp REFERENCES time(start_time) sortkey,
user_id text NOT NULL REFERENCES users(user_id),
level text,
song_id text REFERENCES songs(song_id) distkey,
artist_id text REFERENCES artists(artist_id),
session_id text,
location text,
user_agent text);""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON '{}';
""").format(config.get('S3','LOG_DATA'),
                        config.get('IAM_ROLE', 'ARN'),
                        config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""").format(config.get('S3','SONG_DATA'),
                config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time,
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e, staging_songs s
    WHERE e.page = 'NextSong'
    AND e.song = s.title
    AND user_id NOT IN (SELECT DISTINCT s.user_id FROM songplays s WHERE s.user_id = user_id
                       AND s.start_time = start_time AND s.session_id = session_id )
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        start_time,
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year,
        EXTRACT(weekday from start_time) AS weekday
    FROM (
    	SELECT DISTINCT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time
        FROM staging_events s
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
