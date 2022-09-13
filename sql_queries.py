import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE stage_events (
        artist VARCHAR,
        auth  VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        item_in_session INTEGER,
        last_name VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        session_id INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE stage_songs (
        num_songs INTEGER,
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

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id BIGINT IDENTITY(0,1),
        start_time TIMESTAMP,
        user_id INTEGER,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INT UNIQUE NOT NULL,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    );                  
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR UNIQUE NOT NULL,
        title VARCHAR NOT NULL,
        artist_id VARCHAR,
        year INT,
        duration FLOAT NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR UNIQUE NOT NULL,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP UNIQUE NOT NULL,
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
    COPY stage_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {} REGION 'us-west-2'
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY stage_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto' REGION 'us-west-2'
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time, se.user_id as user_id, se.level as level,
            ss.song_id as song_id, ss.artist_id as artist_id, se.session_id as session_id,
            se.location as location, se.user_agent as user_agent
    FROM stage_events se, stage_songs ss
    WHERE (se.song = ss.title) AND (se.artist = ss.artist_name) AND (page = 'NextSong');
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM stage_events
    WHERE (user_id IS NOT NULL) AND (page = 'NextSong');
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM stage_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name AS name, artist_location as location,
            artist_latitude as latitude, artist_longitude as longitude
    FROM stage_songs
    WHERE artist_id IS NOT NULL;
""")
        
time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time, EXTRACT(HOUR FROM start_time) as hour,
            EXTRACT(DAY FROM start_time) as day, EXTRACT(WEEK FROM start_time) as week,
            EXTRACT(MONTH FROM start_time) as month, EXTRACT(YEAR FROM start_time) as year,
            EXTRACT(DOW FROM start_time) as weekday
    FROM stage_events
    WHERE page = 'NextSong';
""")
        

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


