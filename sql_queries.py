import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# creating staging tables design, the same as source json files
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist text,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession integer,
        lastName varchar,
        length text,
        level varchar,
        location text,
        method varchar,
        page varchar,
        registration text,
        sessionId varchar,
        song text,
        status integer,
        ts bigint,
        userAgent text,
        userId integer 
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
    num_songs integer,
    artist_id text,
    artist_latitude text,
    artist_longitude text,
    artist_location text,
    artist_name text,
    song_id varchar,
    title text,
    duration text,
    year integer
    )
""")
# star schema tables: dimensions and fact table
user_table_create = ("""
    CREATE TABLE users (
    user_id integer PRIMARY KEY sortkey, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE songs (
    song_id varchar(50) PRIMARY KEY sortkey distkey, 
    title text, 
    artist_id varchar(25), 
    year integer, 
    duration NUMERIC(10,5)
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
    artist_id text PRIMARY KEY sortkey, 
    name text, 
    location text,
    latitude NUMERIC(10,5),
    longitude NUMERIC(10,5)
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE time (
    start_time timestamp PRIMARY KEY sortkey, 
    hour integer NOT NULL, 
    day integer NOT NULL, 
    week integer NOT NULL,
    month integer NOT NULL,
    year integer NOT NULL, 
    weekday integer NOT NULL
    )
    diststyle all;
""")


songplay_table_create = ("""
    CREATE TABLE songplays (
    songplay_id integer IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp REFERENCES time (start_time) sortkey,
    user_id integer REFERENCES users (user_id),
    level varchar, 
    song_id varchar(50) REFERENCES songs (song_id) distkey, 
    artist_id text REFERENCES artists (artist_id), 
    session_id varchar, 
    location varchar(455), 
    user_agent text
    )
""")

# LOAD STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}' 
FORMAT as JSON {}
COMPUPDATE OFF
REGION 'us-west-2';""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs 
from {}
credentials 'aws_iam_role={}'
JSON 'auto'
COMPUPDATE OFF
region 'us-west-2';
""").format(SONG_DATA, IAM_ROLE_ARN)


# ETL and INSERT
songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT 
        DISTINCT TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second' as start_time,
        e.userId as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId as session_id,
        s.artist_location as location,
        e.userAgent as user_agent    
    FROM staging_events e
    INNER JOIN staging_songs s 
    ON s.artist_name = e.artist
    AND s.title = e.song
    WHERE e.page = 'NextSong';
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time,
        EXTRACT(HOUR from start_time)::integer as hour,
        EXTRACT(DAY from start_time)::integer as day,
        EXTRACT(WEEK from start_time)::integer as week,
        EXTRACT(MONTH from start_time)::integer as month,
        EXTRACT(YEAR from start_time)::integer as year,
        EXTRACT(DOW from start_time)::integer as weekday
    FROM
        (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time
        FROM staging_events WHERE start_time IS NOT NULL)
    WHERE start_time NOT IN (SELECT DISTINCT start_time from time);
""")

user_table_insert = ("""
        INSERT INTO users (user_id, first_name,last_name, gender, level)
        SELECT
            DISTINCT userId,
            firstName as first_name,
            lastName as last_name, 
            gender, 
            level
        FROM staging_events
        WHERE page = 'NextSong' AND userId IS NOT NULL
        AND userId NOT IN (SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""
    INSERT INTO songs (song_id,title,artist_id,year,duration)
    SELECT 
        song_id,
        title, 
        artist_id, 
        year, 
        duration::DECIMAL(10,5)
    FROM staging_songs
    WHERE song_id IS NOT NULL 
    AND song_id NOT IN (SELECT DISTINCT song_id from songs);
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,name,location,latitude,longitude)
    SELECT 
        DISTINCT artist_id, 
        artist_name as name, 
        artist_location as location, 
        artist_latitude::DECIMAL(9,6) as latitude,
        artist_longitude::DECIMAL(9,6) as longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL 
    AND artist_id NOT IN (SELECT DISTINCT artist_id from artists);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
