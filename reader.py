import csv
import logging
import sqlite3

from os import listdir
from os.path import isfile, join

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

def create_albums_table():
    Create_Albums_Query = '''CREATE TABLE if not Exists swift_albums
    (entity_index int, entity_id int, album_name varchar(255) NOT NULL)'''

    # 2. Create database
    logging.info ("Connecting to DB...")
    connection=sqlite3.connect('swift_stats.db')
    cursor=connection.cursor()

    logging.info ("Creating a table...")
    # 3. Execute table query to create tables
    cursor.execute(Create_Albums_Query)

    # 4. Parse csv data
    logging.info ("Reading csv...")
    with open('data/Albums.csv' , 'r') as csvfile:
        # create the object of csv.reader()
        csv_file_reader = csv.reader(csvfile, delimiter=',')
        counter = 0
        for row in csv_file_reader:
            if counter == 0:
                # skip the first line, it's a header
                counter += 1
                continue
            entity_index = row[0]
            entity_id = row[1]
            album_name = row[2]
            # print (index, entity_id, album_name)

            # 5. Insert data into a new record
            InsertQuery=f'''INSERT INTO swift_albums VALUES 
            ('{entity_index}','{entity_id}','{album_name}')'''
            logging.info ("Adding row...%s" % InsertQuery)

            # 6. Execute the insert
            cursor.execute(InsertQuery)
        
    logging.info ("Commiting and closing connection.")
    # 7. Commit and close the connection
    connection.commit()
    connection.close()


def create_tracks_table():
    Create_Tracks_Query = '''CREATE TABLE if not Exists swift_tracks
    (entity_index int, entity_id int, album_name varchar(255) NOT NULL, track_name varchar(255) NOT NULL, PRIMARY KEY(entity_index))'''

    # 2. Create database
    logging.info ("Connecting to DB...")
    connection=sqlite3.connect('swift_stats.db')
    cursor=connection.cursor()

    logging.info ("Creating a table...")
    # 3. Execute table query to create tables
    cursor.execute(Create_Tracks_Query)


    tracks_path = 'data/Tabular'
    onlyfiles = [f for f in listdir(tracks_path) if isfile(join(tracks_path, f))]
    index = 0
    for f in onlyfiles:
        path = join(tracks_path, f)
        with open(path, 'r') as csvfile:
            # create the object of csv.reader()
            csv_file_reader = csv.reader(csvfile, delimiter=',')
            counter = 0
            album_name = f.split(".")[0]
            for row in csv_file_reader:
                if counter == 0:
                    # skip the first line, it's a header
                    counter += 1
                    continue
                entity_index = index
                index += 1
                entity_id = row[1]
                album_name = album_name
                track_name = row[2]
                print (entity_index, entity_id, album_name, track_name)

                # 5. Insert data into a new record
                InsertQuery=f'''INSERT INTO swift_tracks VALUES 
                ('{entity_index}', '{entity_id}','{album_name}', '{track_name}')'''
                logging.info ("Adding row...%s" % InsertQuery)

                # 6. Execute the insert
                cursor.execute(InsertQuery)
        
    logging.info ("Commiting and closing connection.")
    # 7. Commit and close the connection
    connection.commit()
    connection.close()

def list_albums():
    logging.info ("Connecting to DB...")
    connection=sqlite3.connect('swift_stats.db')
    cursor = connection.execute("SELECT * from swift_albums")
    for row in cursor:
        print (row[0], row[1], row[2])

# create_albums_table()
create_tracks_table()

# list_albums()
