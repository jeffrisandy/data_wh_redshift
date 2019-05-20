import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

dbuser = config.get("CLUSTER", "DB_USER")
ARN = config.get("IAM_ROLE", "ARN")
song_path = config.get("S3", "SONG_DATA")
log_data = config.get("S3", "LOG_DATA")
log_jsonpath = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events 
                            (
                                artist_name varchar,
                                auth varchar,
                                first_name varchar,
                                gender varchar, 
                                itemInSession int,
                                last_name varchar,
                                length float,
                                level varchar,
                                location varchar, 
                                method varchar,
                                page varchar,
                                registration float,
                                sessionId int,
                                song_name varchar,
                                status int,
                                timestamp varchar, 
                                userAgent varchar,
                                userID int
                            );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs 
                            (
                                num_songs int, 
                                artist_id varchar,
                                artist_latitude float,
                                artist_longitude float,
                                artist_location varchar,
                                artist_name varchar,
                                song_id varchar,
                                title varchar,
                                duration float,
                                year int
                            );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
                            (
                                songplay_id INT IDENTITY(0,1),
                                start_time timestamp NOT NULL,
                                user_id int NOT NULL,
                                level varchar NOT NULL,
                                song_id varchar NULL, 
                                artist_id varchar NULL, 
                                session_id int NOT NULL,
                                location varchar NOT NULL,
                                user_agent varchar NOT NULL
                                
                            );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                            (
                                user_id int PRIMARY KEY,
                                first_name varchar NOT NULL,
                                last_name varchar NOT NULL,
                                gender varchar NOT NULL,
                                level varchar NOT NULL
                            );
                                
""")

song_table_create = song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
                            (
                                song_id varchar PRIMARY KEY,
                                title varchar NOT NULL,
                                artist_id varchar NOT NULL,
                                year int NOT NULL,
                                duration float NOT NULL
                             );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                            (
                                artist_id varchar PRIMARY KEY,
                                name varchar NOT NULL, 
                                location varchar NULL, 
                                latitude float NULL,
                                longitude float NULL
                            );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                            (
                                start_time timestamp PRIMARY KEY,
                                hour int NOT NULL,
                                day int NOT NULL,
                                week int NOT NULL,
                                month int NOT NULL,
                                year int NOT NULL,
                                weekday int NOT NULL
                            );
""")

# STAGING TABLES

staging_songs_copy = f"""copy staging_songs from {song_path}
iam_role {ARN}
FORMAT AS JSON 'auto'
compupdate off STATUPDATE OFF
region 'us-west-2';
"""


staging_events_copy = f"""COPY staging_events FROM {log_data} 
iam_role {ARN} 
FORMAT AS JSON {log_jsonpath}
compupdate off STATUPDATE OFF
region 'us-west-2';
"""

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT
        TIMESTAMP 'epoch' + e.timestamp/1000 *INTERVAL '1 second' as start_time, 
        e.userID, 
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events e
    JOIN staging_songs s
    ON lower(e.artist_name) = lower(s.artist_name) AND  lower(e.song_name) = lower(s.title)
    WHERE e.page = 'NextSong' AND e.userID IS NOT NULL
""")
# ON lower(e.artist_name) = lower(s.artist_name) AND e.length = s.duration AND lower(e.song_name) = lower(s.title)

user_table_insert =  ("""INSERT INTO users (user_id, first_name, last_name, gender, level)  
    SELECT DISTINCT 
        userID,
        first_name,
        last_name,
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong' AND userID IS NOT NULL 
""")

song_table_insert =  ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year, 
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
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
        SELECT DISTINCT  TIMESTAMP 'epoch' + timestamp/1000 *INTERVAL '1 second' as start_time 
        FROM staging_events s
        WHERE s.page = 'NextSong' AND s.userID IS NOT NULL 
    )
    WHERE start_time IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
