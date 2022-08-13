import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

HOST                   = config.get("CLUSTER","HOST")
DWH_DB                 = config.get("CLUSTER","DWH_DB")
DWH_DB_USER            = config.get("CLUSTER","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DWH_DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DWH_PORT")

S3_LOG_DATA  = config.get("S3", "LOG_DATA")
S3_LOG_PATH  = config.get("S3", "LOG_JSONPATH")
S3_SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE  = config.get('IAM_ROLE', 'ARN')
FORMAT_EVENT = S3_LOG_PATH
FORMAT_SONG = 'auto'
REGION = 'us-west-2'

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events; "
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs; "
songplay_table_drop = "DROP TABLE IF EXISTS songplay; "
user_table_drop = "DROP TABLE IF EXISTS users; "
song_table_drop = "DROP TABLE IF EXISTS song; "
artist_table_drop = "DROP TABLE IF EXISTS artist; "
time_table_drop = "DROP TABLE IF EXISTS time; "

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist         TEXT,
        auth           TEXT,
        firstName      TEXT,
        gender         TEXT,
        itemInSession  INTEGER,
        lastName       TEXT,
        length         FLOAT,
        level          TEXT,
        location       TEXT,
        method         TEXT,
        page           TEXT,
        registration   TEXT,
        sessionId      INTEGER,
        song           TEXT,
        status         INTEGER,
        ts             BIGINT,
        userAgent      TEXT,
        userId         INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs            INTEGER,
        artist_id            TEXT,
        artist_latitude      FLOAT,
        artist_longitude     FLOAT,
        artist_location      TEXT,
        artist_name          TEXT,
        song_id              TEXT,
        title                TEXT,
        duration             FLOAT,
        year                 INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
    (
        songplay_id    BIGINT IDENTITY(1, 1) PRIMARY KEY,
        start_time     TIMESTAMP NOT NULL SORTKEY,
        user_id        INTEGER NOT NULL DISTKEY,
        level          TEXT,
        song_id        TEXT,
        artist_id      TEXT,
        session_id     BIGINT,
        location       TEXT,
        user_agent     TEXT
    ) diststyle key;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id INTEGER PRIMARY KEY SORTKEY, 
        first_name TEXT, 
        last_name TEXT, 
        gender TEXT, 
        level TEXT
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song 
    (
        song_id TEXT PRIMARY KEY SORTKEY, 
        title TEXT NOT NULL, 
        artist_id TEXT, 
        year INTEGER, 
        duration FLOAT NOT NULL
    ) diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist 
    (
        artist_id TEXT PRIMARY KEY SORTKEY, 
        name TEXT NOT NULL, 
        location TEXT, 
        latitude FLOAT, 
        longitude FLOAT
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time 
    (
        start_time TIMESTAMP PRIMARY KEY SORTKEY, 
        hour INTEGER, 
        day INTEGER, 
        week INTEGER, 
        month INTEGER, 
        year INTEGER, 
        weekday INTEGER
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {} 
    iam_role {}
    region       '{}'
    format       as JSON {}
""").format(S3_LOG_DATA, IAM_ROLE, REGION, FORMAT_EVENT)

staging_songs_copy = ("""
    copy staging_songs from {} 
    iam_role {}
    region       '{}'
    format       as JSON '{}'
""").format(S3_SONG_DATA, IAM_ROLE, REGION, FORMAT_SONG)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id , level , song_id , artist_id , session_id , location , user_agent)
    SELECT TIMESTAMP 'epoch' + (se.ts/1000 * INTERVAL '1 second') as start_time,
           se.userId                                              as user_id,
           se.level                                               as level,
           ss.song_id                                             as song_id,
           ss.artist_id                                           as artist_id,
           se.sessionId                                           as session_id,
           se.location                                            as location,
           se.userAgent                                           as user_agent
    FROM staging_events se
    JOIN staging_songs ss 
    ON se.song = ss.title 
    AND se.artist = ss.artist_name
    AND se.page = 'NextSong'
    AND se.length = ss.duration;
""")

user_table_insert = ("""
    INSERT INTO users (user_id , first_name , last_name, gender , level)
    SELECT DISTINCT (se.userId)                                   as user_id,
           se.firstName                                           as first_name,
           se.lastName                                            as last_name,
           se.gender                                              as gender,
           se.level                                               as level
    FROM staging_events se
    WHERE se.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO song (song_id , title , artist_id , year , duration)
    SELECT DISTINCT (ss.song_id)                                  as song_id,
           ss.title                                               as title,
           ss.artist_id                                           as artist_id,
           ss.year                                                as year,
           ss.duration                                            as duration
    FROM staging_songs ss;
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id , name , location , latitude , longitude)
    SELECT DISTINCT (ss.artist_id)                                as artist_id,
           ss.artist_name                                         as name,
           ss.artist_location                                     as location,
           ss.artist_latitude                                     as latitude,
           ss.artist_longitude                                    as longitude
    FROM staging_songs ss;
""")

time_table_insert = ("""
    INSERT INTO time (start_time , hour , day , week , month , year , weekday)
    WITH temp AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
    SELECT DISTINCT ts                                           as start_time,
    extract(hour from ts)                                        as hour,
    extract(day from ts)                                         as day,
    extract(week from ts)                                        as week,
    extract(month from ts)                                       as month,
    extract(year from ts)                                        as year,
    extract(weekday from ts)                                     as weekday
    FROM temp;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
