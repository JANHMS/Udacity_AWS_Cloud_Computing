import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
# CASCADE
# Clause that indicates to automatically drop objects that depend on the view, such as other views.

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist          VARCHAR,
auth            VARCHAR,
firstName       VARCHAR,
gender          VARCHAR,
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR,
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id            VARCHAR,
num_songs          INTEGER,
title              VARCHAR,
artist_name        VARCHAR,
artist_latitude    FLOAT,
year               INTEGER,
duration           FLOAT,
artist_id          VARCHAR,
artist_longitude   FLOAT,
artist_location    VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id int PRIMARY KEY sortkey,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
    )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id VARCHAR PRIMARY KEY sortkey,
title varchar,
artist_id VARCHAR,
year int,
duration float
    )
""")


artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR PRIMARY KEY sortkey,
    name varchar,
    location varchar,
    latitude numeric,
    longitude numeric
    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY sortkey distkey,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
    songplay_id INT primary key sortkey,
    start_time timestamp distkey,
    user_id int,
    level varchar,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id int,
    location varchar,
    user_agent varchar,
    CONSTRAINT fk_time
        FOREIGN KEY (start_time) REFERENCES time(start_time),
    CONSTRAINT fk_users
        FOREIGN KEY (user_id) REFERENCES users(user_id),
    CONSTRAINT fk_songs
        FOREIGN KEY (song_id) REFERENCES songs(song_id),
    CONSTRAINT fk_artists
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    IAM_ROLE '{}'
    COMPUPDATE OFF REGION '{}'
    FORMAT  JSON {}
    TIMEFORMAT  'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['REGION'],
    config['S3']['LOG_JSONPATH']
)


staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role '{}'
    json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])



staging_events_copy = ("""
COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH']
)

staging_songs_copy = ("""
COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'])



# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (SELECT DISTINCT
    a.ts ,
    a.userId ,
    a.level ,
    b.song_id ,
    CAST(b.artist_id AS INT),
    a.sessionId ,
    a.location ,
    a.userAgent
    FROM staging_events a
        LEFT JOIN staging_songs b
        ON a.song = b.title AND a.artist = b.artist_name and a.length = b.duration
    WHERE a.page = 'NextSong')
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)(
    userId ,
    firstName ,
    lastName ,
    gender,
    level
    FROM staging_events
    WHERE userId IS NOT NULL
    AND page = 'NextSong'
    )
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)(
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
    )
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)(
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)(
    ts,
    EXTRACT(hour FROM ts),
    EXTRACT(day FROM ts),
    EXTRACT(week FROM ts),
    EXTRACT(month FROM ts),
    EXTRACT(year FROM ts),
    EXTRACT(dayofweek FROM ts)
    FROM staging_events
    WHERE ts IS NOT NULL)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
