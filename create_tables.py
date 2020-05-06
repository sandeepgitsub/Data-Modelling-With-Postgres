############################################################################################################
#             CHANGE LOG
# This script extracts the data from songs and log files transforms the data and loads in to
# Dimenstional (songs, artists, users , time ) and Fact (songsplay) table
#
# Below outlines the flow of the script.
# 1) import the required libraries and sqlqueries from sql_queries script 
# 2) Create a database connection  
# 3) Create a cursor using the connection
# 4) create the tables
# 5) close the database connection.
############################################################################################################
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """ connect to a default database , drop sparkifydb database if exisits and create sparkifydb database with utf8 encoding,
    close the connection and connect to sparkifydb and create the crusor. Return the cursor and database connection"""
    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        
    # create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
        conn.close()    
    
    # connect to sparkify database
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    # set the autocommit to true    
        conn.set_session(autocommit=True)
        return cur, conn
    except psycopg2.Error as e:
        print("Error in create database function")
        print(e)
        return None, None


def drop_tables(cur, conn):
    """ drop tables using the list from sql queries module"""
    # loop through the list and excute the sql statement
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(" dropping of table using " + query + " not successful")
            print(e)

def create_tables(cur, conn):
    """ create tables using the list from sql queries module"""
    # loop through the list and excute the sql statement
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(" creating of table using " + query + " not successful")
            print(e)


def main():
    """ create the cur and connection , create the tables  and close the connection"""
    try:
        cur, conn = create_database()

        if cur != None and conn != None:
    #         drop_tables(cur, conn)
            create_tables(cur, conn)

            conn.close()
        else:
            print(" Not executing drop tables and create table as creation/connection of Database and curr  is not successful")
    except Exception as e:
        print(" exception in main script and the error is " + str(e))
        

if __name__ == "__main__":
    main()