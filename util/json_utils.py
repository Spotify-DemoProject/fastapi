import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.join(current_dir, f'../lib')
data_dir = os.path.join(current_dir, f'../data')
sys.path.append(lib_dir)

from os_libs import *
from spotify_libs import *
from postgresql_libs import *
from threading import Thread
from time import time, sleep
from math import ceil
import json

def thread_artists(insert_date:str):
    check_mkdirs(dir=f"{data_dir}/artists/{insert_date}")

    query_search = f"""
                   SELECT artist_id FROM artists
                   WHERE insert_date = '{insert_date}'
                   """
    result = fetchall_query(QUERY=query_search)
    id_list = [artist[0] for artist in result]
    
    print(len(id_list))
    
    def do_work(id_list_thread, insert_date, cnt):
        list_50 = [id_list_thread[i:i+50] for i in range(0, len(id_list_thread), 50)]
        
        for index in range(len(list_50)):
            start_time = time()
            endpoint = "artists"
            params = {'ids': ','.join(list_50[index]), 'market' : 'KR'}
            
            response = get_response(cnt=cnt, endpoint=endpoint, params=params)

            file_dir = f"{data_dir}/artists/{insert_date}/artists_{cnt}_{index}.json"
            
            try :
                with open(file_dir, "a") as file:
                    json.dump(response, file, indent=4)
            except:
                with open(file_dir, "w") as file:
                    json.dump(response, file, indent=4)

            end_time = time()
            remain_time = 2.0 - (end_time - start_time)
            sleep(remain_time) if remain_time > 0 else sleep(0)   

    num_threads = 12
    artists_per_thread = ceil(len(id_list) / num_threads)
    thread_list = [id_list[i:i+artists_per_thread] for i in range(0, len(id_list), artists_per_thread)]

    threads = []
    for idx, artist_ids in enumerate(thread_list):
        thread = Thread(target=do_work, args=(artist_ids, insert_date, idx+1))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def thread_albums_b(insert_date:str):
    check_mkdirs(dir=f"{data_dir}/albums/{insert_date}")

    query_search = f"""
                   SELECT album_id FROM albums
                   WHERE insert_date = '{insert_date}'
                   """
    result = fetchall_query(QUERY=query_search)
    id_list = [album[0] for album in result]

    def do_work(id_list, insert_date, cnt):
        list_20 = [id_list[i:i+20] for i in range(0, len(id_list), 20)]
        
        for index in range(len(list_20)):
            start_time = time()
            endpoint = "albums"
            params = {'ids': ','.join(list_20[index]), 'market' : 'KR'}
            
            response = get_response(cnt=cnt, endpoint=endpoint, params=params)

            file_dir = f"{data_dir}/albums/{insert_date}/albums_{cnt}_{index}.json"
            
            try :
                with open(file_dir, "a") as file:
                    json.dump(response, file, indent=4)
            except:
                with open(file_dir, "w") as file:
                    json.dump(response, file, indent=4)

            end_time = time()
            remain_time = 2.0 - (end_time - start_time)
            sleep(remain_time) if remain_time > 0 else sleep(0)   

    num_threads = 12
    albums_per_thread = ceil(len(id_list) / num_threads)
    thread_list = [id_list[i:i+albums_per_thread] for i in range(0, len(id_list), albums_per_thread)]

    threads = []
    for idx, album_ids in enumerate(thread_list):
        thread = Thread(target=do_work, args=(album_ids, insert_date, idx+1))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def thread_albums(insert_date:str):
    check_mkdirs(dir=f"{data_dir}/albums/{insert_date}")

    query_search = f"""
                   SELECT album_id FROM albums
                   WHERE insert_date = '{insert_date}'
                   """
    result = fetchall_query(QUERY=query_search)
    id_list = [album[0] for album in result]

    def do_work(id_list, insert_date, cnt):
        list_20 = [id_list[i:i+20] for i in range(0, len(id_list), 20)]
        
        for index in range(len(list_20)):
            start_time = time()
            endpoint = "albums"
            params = {'ids': ','.join(list_20[index]), 'market' : 'KR'}
            
            response = get_response(cnt=cnt, endpoint=endpoint, params=params)

            file_dir = f"{data_dir}/albums/{insert_date}/albums_{cnt}_{index}.json"
            
            try :
                with open(file_dir, "a") as file:
                    json.dump(response, file, indent=4)
            except:
                with open(file_dir, "w") as file:
                    json.dump(response, file, indent=4)
                    
            for album in response['albums']:
                for track in album['tracks']['items']:
                    track_id = track['id']
                    QUERY = """
                        INSERT INTO tracks (track_id, insert_date)
                        VALUES (%s, %s)
                        ON CONFLICT (track_id) DO NOTHING;
                    """
                    VALUES = (track_id, insert_date)
                    execute_query(QUERY=QUERY, VALUES=VALUES)

            end_time = time()
            remain_time = 2.0 - (end_time - start_time)
            sleep(remain_time) if remain_time > 0 else sleep(0)   

    num_threads = 12
    albums_per_thread = ceil(len(id_list) / num_threads)
    thread_list = [id_list[i:i+albums_per_thread] for i in range(0, len(id_list), albums_per_thread)]

    threads = []
    for idx, album_ids in enumerate(thread_list):
        thread = Thread(target=do_work, args=(album_ids, insert_date, idx+1))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def thread_tracks(insert_date:str):
    check_mkdirs(dir=f"{data_dir}/tracks/main/{insert_date}")
    check_mkdirs(dir=f"{data_dir}/tracks/audio_features/{insert_date}")

    query_search = f"""
                   SELECT track_id FROM tracks
                   WHERE insert_date = '{insert_date}'
                   """
    result = fetchall_query(QUERY=query_search)
    id_list = [album[0] for album in result]

    def do_work(id_list_thread, insert_date, cnt):
        list_100 = [id_list_thread[i:i+100] for i in range(0, len(id_list_thread), 100)]
        
        for index in range(len(list_100)):
            start_time = time()
            endpoint = "audio-features"
            params = {'ids': ','.join(list_100[index]), 'market' : 'KR'}
            
            response = get_response(cnt=cnt, endpoint=endpoint, params=params)

            file_dir = f"{data_dir}/tracks/audio_features/{insert_date}/tracks_{cnt}_{index}.json"
            
            try :
                with open(file_dir, "a") as file:
                    json.dump(response, file, indent=4)
            except:
                with open(file_dir, "w") as file:
                    json.dump(response, file, indent=4)

            end_time = time()
            remain_time = 2.0 - (end_time - start_time)
            sleep(remain_time) if remain_time > 0 else sleep(0)
            
            start_time = time()
            endpoint = "tracks"
            params = {'ids': ','.join(list_100[index][:50]), 'market' : 'KR'}
            
            response = get_response(cnt=cnt, endpoint=endpoint, params=params)

            file_dir = f"{data_dir}/tracks/main/{insert_date}/tracks_{cnt}_{index}_1.json"
            
            try :
                with open(file_dir, "a") as file:
                    json.dump(response, file, indent=4)
            except:
                with open(file_dir, "w") as file:
                    json.dump(response, file, indent=4)

            end_time = time()
            remain_time = 2.0 - (end_time - start_time)
            sleep(remain_time) if remain_time > 0 else sleep(0)            

            start_time = time()
            endpoint = "tracks"
            params = {'ids': ','.join(list_100[index][50:]), 'market' : 'KR'}
            
            response = get_response(cnt=cnt, endpoint=endpoint, params=params)

            file_dir = f"{data_dir}/tracks/main/{insert_date}/tracks_{cnt}_{index}_2.json"
            
            try :
                with open(file_dir, "a") as file:
                    json.dump(response, file, indent=4)
            except:
                with open(file_dir, "w") as file:
                    json.dump(response, file, indent=4)

            end_time = time()
            remain_time = 2.0 - (end_time - start_time)
            sleep(remain_time) if remain_time > 0 else sleep(0)   
            
    num_threads = 12
    artists_per_thread = ceil(len(id_list) / num_threads)
    thread_list = [id_list[i:i+artists_per_thread] for i in range(0, len(id_list), artists_per_thread)]

    threads = []
    for idx, artist_ids in enumerate(thread_list):
        thread = Thread(target=do_work, args=(artist_ids, insert_date, idx+1))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    # thread_artists(insert_date="2023-12-29")
    thread_albums(insert_date="2024-01-06")
    # thread_tracks(insert_date="2023-12-29")
    