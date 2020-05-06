############################################################################################################
#             CHANGE LOG
# This script extracts the data from songs and log files transforms the data and loads in to
# Dimenstional (songs, artists, users , time ) and Fact (songsplay) table
#
# Below outlines the flow of the script.
# 1) import the required libraries and sqlqueries from sql_queries script 
# 2) Create a database connection  
# 3) Create a cursor using the connection
# 4) Process songs files and insert the data in to songs and artists table
# 5) Process log files extarct data only for 'NextSong' action load users and time tables
# 6) Insert the records to songplay fact table using log file and by getting song id and artist id from songs and 
#    artists table
# 7) Do a quality check to see no of records in tables are correct.
# 8) close the connection of the database
############################################################################################################
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime, timedelta



def process_song_file(cur, filepath):
    """ Extarct the data from each song file and insert the data in to songs and artists tables"""
    try:
        # open song file
        df = pd.read_json(filepath, lines = True)
        
        # get the number of records and add to the global variable
        global no_of_records_song_files
        no_of_records_song_files = no_of_records_song_files + df.shape[0]
        
        # insert song record
        songs_values = df.values[0]
        song_data = list([songs_values[7],songs_values[8], songs_values[0], songs_values[9],  songs_values[5]])
        cur.execute(song_table_insert, song_data)

        # insert artist record
        artist_data = list([songs_values[0], songs_values[4], songs_values[2], songs_values[1], songs_values[3]])
        cur.execute(artist_table_insert, artist_data)
    except Exception as e:
        print(" Exception in process song file fucntion and the error is: " + str(e))

def process_log_file(cur, filepath):
    """ Extarct the data from each log file and insert the data in to users and time tables
    also extract song id, artist id from songs and artists table and insert data in to songplay table along with other details
    from log file"""
    try:
        # open log file
        df = pd.read_json(filepath, lines= True)

        # filter by NextSong action
        df = df[df['page'] == 'NextSong']

        #get the count of the valid records in the file and add to the global variable
        global no_of_records_log_files
        no_of_records_log_files = no_of_records_log_files + df.shape[0]
        
        # convert timestamp column to datetime
        df['ts']= t = df['ts'].apply(lambda x : datetime.fromtimestamp(x/1000.0))


        # insert time data records
        time_data =  [[x, x.hour, x.day, x.week, x.month, x.year, x.weekday()] for i, x in t.iteritems()]
        column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday' ]
        time_df = pd.DataFrame.from_records(time_data, columns=column_labels )

        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))

        # load user table
        user_df = df [['userId', 'firstName','lastName', 'gender', 'level']]

        # insert user records
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)

        # insert songplay records
        for index, row in df.iterrows():

            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # insert songplay record
            songplay_data = ( row['ts'], row['userId'], row['level'],songid, artistid, row['sessionId'], row['location'], row['userAgent'])
            cur.execute(songplay_table_insert, songplay_data)
    except Exception as e:
        print(" Exception in process log file fucntion and the error is: " + str(e))

def process_data(cur, conn, filepath, func):
    """ process the json files in the directory and excute the fucntion passed as argument on the files from the directory"""
    try:
        # get all files matching extension from directory
        all_files = []
        for root, dirs, files in os.walk(filepath):
            files = glob.glob(os.path.join(root,'*.json'))
            for f in files :
                all_files.append(os.path.abspath(f))

        # get total number of files found
        num_files = len(all_files)
        print('{} files found in {}'.format(num_files, filepath))

        # iterate over files and process
        for i, datafile in enumerate(all_files, 1):
            func(cur, datafile)
            conn.commit()
            print('{}/{} files processed.'.format(i, num_files))
    except Exception as e:
        print(" Exception in process data fucntion and the error is: " + str(e))
    

def files_tables_counts_match(cur, tables_names_list, no_of_records_file):
    """ extarct the records count from table and compare with file count"""
    try:
        for table in tables_names_list:
            # get the number of records
            cur.execute(no_of_records_select + table)
            table_count = cur.fetchone()
            print( ' no of records in {} is {} and no of records in files is {}'.format( table,table_count[0],no_of_records_file ))
            
            # match the file count and table count
            if table_count[0] == no_of_records_file:
                print('record count of {} matches with files'.format(table))
            else:
                print('record count of {} does not match with files'.format(table))
    except Exception as e:
        print(" Exception in files counts match fucntion and the error is: " + str(e))


def tables_count_match(cur, distinct_from_table, distinct_field, second_table):
    """ extarct the records count from tables and compare the counts """
    try:
        # get the distinct field count for a table
        cur.execute('select count(DISTINCT ' + distinct_field + ') from ' + distinct_from_table )
        table1_count= cur.fetchone()
        
        # get the count for a table
        cur.execute('select count(*) from ' + second_table )
        table2_count= cur.fetchone()
        
        print('No of records from {} is {} and no of records from {} is {}'.format(distinct_from_table, str(table1_count[0]), second_table, table2_count[0] ))
        
        # match the tables counts
        if table1_count[0] == table2_count[0] :
            print(' Tables Count MATCH')
        else:
            print('Tables Count DO NOT MATCH')
    except Exception as e:
        print(" Exception in tables counts match fucntion and the error is: " + str(e))
        
        
def main():
    """ main fucntion to connect the database, create the cursor , process songs and log directories to extracts files
    and load the fact , dimension tables , check the records count and close the database connection"""
    try:
        # lists with tables name direclty loaded from song and log file
        tables_from_song_file = ['songs']
        tables_from_log_file = ['songplays']
                
        # create a database connection for sparkifydb database and create a cursor using the same connection
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()

        # process the data from songs and log folder
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
        # match the records counts of files and tables
        files_tables_counts_match(cur, tables_from_song_file, no_of_records_song_files)
        files_tables_counts_match(cur, tables_from_log_file, no_of_records_log_files)
        
        # match the records counts of tables with record count of tables
        tables_count_match(cur, 'songs', 'artist_id', 'artists')
        tables_count_match(cur, 'songplays', 'user_id', 'users')
        tables_count_match(cur, 'songplays', 'start_time', 'time')
        
        # close the database connection
        conn.close()
    except Exception as e:
        print(" Exception in main fucntion and the error is: " + str(e))        
        

if __name__ == "__main__":

    # define the global variables
    no_of_records_song_files = 0
    no_of_records_log_files = 0
    
    # excute the main function
    main()