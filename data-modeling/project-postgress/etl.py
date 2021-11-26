import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def get_files(filepath):
    """Returns an array of files in a given filepath

    Parameters:
    argument1 (filepath): string

    Returns:
    an string array of .json files

    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_song_file(cur, filepath):
     """processes a song file a according to a filepath, 
        extracts song data and stores it into song table,
         extracts artist data and stores it into artist table

        Parameters:
        argument1 (filepath): string

    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
    song_data = list(song_data.values[0])

    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_longitude", "artist_latitude"]]
    artist_data = list(artist_data.values[0])
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
      """processes a log file a according to a filepath, 
         generates timedata from a given timestamp and stores it into time table
         extracts user data and stores it into user table
         combines data and saves into songplay table

        Parameters:
        argument1 (filepath): string

    """
    # open log file
    
    log_files = filepath
    df = pd.read_json(filepath, lines=True)

    
    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    # time_data = 
    # column_labels = 
    time_df = pd.DataFrame(columns = ['timestamp', 'hour', 'day', 'week']) 

    time_df['timestamp'] = df['ts']
    time_df['hour']= t.dt.hour
    time_df['day']= t.dt.day
    time_df['week']= t.dt.week
    time_df['month']= t.dt.month
    time_df['year']=t.dt.year
    time_df['weekday']=t.dt.weekday

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame(columns = ['user_id', 'first_name', 'last_name', 'gender', 'level'])
    user_df['user_id']=df['userId']
    user_df['first_name']=df['firstName']
    user_df['last_name']=df['lastName']
    user_df['gender']=df['gender']
    user_df['level']=df['level']

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
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), 
                         row.userId,
                         row.level,
                         songid,
                         artistid,
                         row.sessionId,
                         row.location,
                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ processes data from a given database connection, 
        gets the data from a given filepath and processes it with a given function (func), 
    """
    
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


def main():
    """
    opens a db connection and processes data for song_file and log_file
    closes connection at the end
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()