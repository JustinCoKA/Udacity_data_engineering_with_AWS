import configparser
import os

# CONFIG
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'dwh.cfg')
config.read(config_path)

# DROP TABLES

# Saving log_data temporary
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
# Saving song_data temporary
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist TEXS,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    itemInSession INT,
    lastName TEXT,
    length FLOAT,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration BIGINT,
    sessionId INT,
    song TEXT,
    status TEXT,
    ts BIGINT,
    userAgent TEXT,
    userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id TEXT,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location TEXT,
    artist_name TEXT,
    song_id TEXT,
    title TEXT,
    duration FLOAT,
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level TEXT,
    song_id TEXT,
    artist_id TEXT,
    session_id INT,
    location TEXT,
    user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT IDENTITY(0,1) PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT PRIMARY KEY,
    title TEXT,
    artist_id TEXT,
    year INT,
    duration FLOAT
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    name TEXT,
    location TEXT,
    latitude FLOAT,
    longitude FLOAT
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    JSON '{}'
    REGION '{}'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL;
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH'),
    config.get('CLUSTER', 'REGION'))

staging_songs_copy = ("""
    COPY staging_song
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION '{}'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL;
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('CLUSTER', 'REGION')
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second' AS start_time,
        se.userId,
        se.level,
        ss.songId,
        ss.artistId,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events AS se
    JOIN staging_songs AS ss ON se.song = ss.title AND se.artist = ss.artist_name WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM staging_events 
    WHERE page = 'NextSong' AND userID is NOT NULL; 
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_song;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_song;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
        start_time,
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time),
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(weekday FROM start_time)
    FROM (
        SELECT TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second' AS start_time,
        FROM staging_events
        WHERE page = 'NextSong'
        )
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert]
