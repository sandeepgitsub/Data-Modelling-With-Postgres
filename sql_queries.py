############################################################################################################
#             CHANGE LOG
# This scripts have all the sql queries used in create tables and etl python scripts
#
# Below outlines the flow of the script.
# 1) Define Drop tables varibles having drop table sql statements
# 2) Define Create tables varibles having create table sql statements
# 3) Define varibles having select sql statements
# 4) Define lists to hold create and drop sql statement variables.
# 
############################################################################################################

# DROP TABLES

songplay_table_drop = "DROP TABLE songplays"
user_table_drop = "DROP TABLE users"
song_table_drop = " DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time "

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
songplay_id SERIAL, 
start_time TIMESTAMP  REFERENCES time(start_time), 
user_id int REFERENCES users(user_id), 
level varchar, 
song_id varchar REFERENCES songs(song_id), 
artist_id varchar REFERENCES artists(artist_id),
session_id int, 
location varchar, 
user_agent text, 
UNIQUE(songplay_id, start_time, user_id, song_id, artist_id ));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
user_id int PRIMARY KEY,
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
song_id varchar PRIMARY KEY,
title varchar, 
artist_id varchar, 
year int, 
duration numeric);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
artist_id varchar PRIMARY KEY, 
name varchar, 
location varchar, 
latitude float, 
longitude float);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP PRIMARY KEY , 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int);
""")

# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO songplays( start_time , user_id , level , song_id , artist_id , session_id , location , user_agent) 
VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = (""" INSERT INTO users(user_id , first_name , last_name , gender , level ) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
DO NOTHING;
""")

song_table_insert = (""" INSERT INTO songs(song_id , title , artist_id , year , duration ) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) 
DO NOTHING;
""")

artist_table_insert = (""" INSERT INTO artists(artist_id , name , location , latitude , longitude ) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) 
DO NOTHING;
""")


time_table_insert = (""" INSERT INTO time(start_time , hour , day , week , month , year , weekday ) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) 
DO NOTHING;
""")

# FIND SONGS

song_select = (""" select song_id, songs.artist_id 
from songs join artists on songs.artist_id = artists.artist_id 
where title = %s and name = %s  and duration = %s
""")

# Get the count of the recotds for the given table
no_of_records_select = (" select count(*) from " )



# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]