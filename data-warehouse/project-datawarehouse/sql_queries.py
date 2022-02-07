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

staging_events_table_create = ("""
CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1),
    artist_name VARCHAR(255),
    auth VARCHAR(50),
    first_name VARCHAR(255),
    gender VARCHAR(1),
    item_in_session INTEGER,
    last_name VARCHAR(255),
    length DOUBLE PRECISION,
    level VARCHAR(255),
    location VARCHAR(255),
    method VARCHAR(25),
    page VARCHAR(255),
    registration VARCHAR(255),
    session_id BIGINT,
    song VARCHAR(255),
    status INTEGER,
    timestamp TIMESTAMP,
    user_agent TEXT,
    user_id INTEGER)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    song_id VARCHAR(100),
    num_songs INTEGER,
    artist_id VARCHAR(100),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration FLOAT,
    year INTEGER)
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR(255),
    song_id VARCHAR(100) NOT NULL,
    artist_id VARCHAR(100) NOT NULL,
    session_id BIGINT,
    location VARCHAR(255),
    user_agent TEXT)
""")

user_table_create = ("""
CREATE TABLE users(
    user_id INTEGER NOT NULL PRIMARY KEY
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(255)
    )
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id VARCHAR(100) NOT NULL PRIMARY KEY,
    title VARCHAR(255),
    artist_id VARCHAR(100),
    year INTEGER,
    duration DOUBLE PRECISION
    )
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id VARCHAR(100) NOT NULL PRIMARY KEY,
    name VARCHAR(255),
    artist_location VARCHAR(255),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION
    )
""")

time_table_create = ("""
CREATE TABLE time(
    start_time timestamp NOT NULL PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    year INTEGER,
    weekday INTEGER
    )
""")


# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    json {};
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_events
        from {}
        iam_role {}
        json 'auto';
    """).format(config.get('S3', 'SONG_DATA'),
                config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

# songplays
songplay_table_insert = ("""
INSERT INTO songplays(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second AS start_time,
       e.user_id,
       e.user_level,
       s.song_id,
       s.artist_id,
       e.session_id,
       e.location,
       e.user_agent
FROM staging_events e, staging_songs s
WHERE e.page = 'NextSong'
AND e.song_title = s.title
AND e.artist_name = s.name
AND e.song_length = s.duration
""")

# users
user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT(user_id),
       first_name,
       last_name,
       gender,
       level
FROM staging_songs
WHERE user_id IS NOT NULL
AND page = 'NextSong';
""")

# songs
song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id),
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

# artists
artist_table_insert = ("""
INSERT INTO artists(artist_id, name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latidude,
    artist_longitude
FROM staging_songs
WHERE artist_is IS NOT NULL
""")

# time
time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, year, weekday)
SELECT  DISTINCT(start_time),
        EXTRACT(hour from start_time) AS hour,
        EXTRACT(day from start_time) AS day,
        EXTRACT(week from start_time) AS week,
        EXTRACT(year from start_time) AS year,
        EXTRACT(weekday from start_time) AS weekday
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]