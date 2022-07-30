import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    # pre-processing dataframe to replace all (') character to (’) for working in sql better.
    df['title'] = df['title'].str.replace("'", "’")
    df['artist_name'] = df['artist_name'].str.replace("'", "’")
    df['artist_location'] = df['artist_location'].str.replace("'", "’")

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].copy()
    song_data = song_data.values
    song_data = song_data.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].copy()
    artist_data = artist_data.values
    artist_data = artist_data.tolist()[0]
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the log information in order to store it into the time table.
    Then it extracts the user information in order to store it into the users table.
    Finally it extracts the songplay information in order to store it into the songplay table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    # pre-processing dataframe to replace all (') character to (’) for working in sql better.
    df['song'] = df['song'].str.replace("'", "’")
    df['location'] = df['location'].str.replace("'", "’")
    df['lastName'] = df['lastName'].str.replace("'", "’")
    df['firstName'] = df['firstName'].str.replace("'", "’")
    df['artist'] = df['artist'].str.replace("'", "’")
    # convert time column to timestamp column.
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df[['ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']].copy()
    t['ts'] = pd.to_datetime(t['ts'], unit='ms')
    
    # insert time data records
    time_data = pd.DataFrame()
    time_data['start_time'] =  t['ts']
    time_data['hour'] =  t['ts'].dt.hour
    time_data['day'] =  t['ts'].dt.day
    time_data['week'] =  t['ts'].dt.week
    time_data['month'] =  t['ts'].dt.month
    time_data['year'] =  t['ts'].dt.year
    time_data['weekday'] =  t['ts'].dt.weekday
    column_labels = time_data.columns.values.tolist()
    time_df = time_data.copy()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()

    # insert user records
    for i, row in user_df.iterrows():
        # check Not Null userId
        if row.userId:
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

        # insert songplay record and check Not Null userId
        if row.userId:
            songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
     """
    This procedure processes a log and song files from directory whose filepath has been provided as an arugment.
    It will do this iteratorly through a file to call func has been provided as an arugment.

    INPUTS: 
    * cur the cursor variable
    * conn the connection variable
    * filepath the file path to the log files, song files.
    * func the function to call
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()