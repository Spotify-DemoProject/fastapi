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

def thread_albums(insert_date:str):
    check_mkdirs(dir=f"{data_dir}/albums/{insert_date}")
    check_mkdirs(dir=f"{data_dir}/tracks/{insert_date}")

    query_search = f"""
                   SELECT album_id FROM albums
                   WHERE insert_date = '{insert_date}'
                   """
    result = fetchall_query(QUERY=query_search)
    id_list = [album[0] for album in result]

    def do_work(id_list, insert_date, cnt):
        for album_id in id_list:
            start_time = time()

            endpoint=f'albums/{album_id}'
            params={'market' : 'KR'}
            response = get_response(cnt=cnt, endpoint=endpoint, params=params)

            file_dir = f"{data_dir}/albums/{insert_date}/{album_id}.json"
            
            try :
                with open(file_dir, "a") as file:
                    json.dump(response, file, indent=4)
            except:
                with open(file_dir, "w") as file:
                    json.dump(response, file, indent=4)

            
            for track in response['tracks']['items']:
                track_id = track["id"]

                file_dir = f"{data_dir}/tracks/{insert_date}/{track_id}.json"
                
                try :
                    with open(file_dir, "a") as file:
                        json.dump(track, file, indent=4)
                except:
                    with open(file_dir, "w") as file:
                        json.dump(track, file, indent=4)

            end_time = time()
            remain_time = 1.5 - (end_time - start_time)
            sleep(remain_time) if remain_time > 0 else sleep(0)   

    num_threads = 10
    artists_per_thread = ceil(len(id_list) / num_threads)
    thread_list = [id_list[i:i+artists_per_thread] for i in range(0, len(id_list), artists_per_thread)]
    
    #TEST
    print(len(thread_list))
    print(">>>>>>>>>>>>")
    for single_list in thread_list:
        print(len(single_list))

    threads = []
    for idx, artist_ids in enumerate(thread_list):
        thread = Thread(target=do_work, args=(artist_ids, insert_date, idx+1))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def thread_artists(insert_date:str):
    check_mkdirs(dir=f"{data_dir}/artists/{insert_date}")

    query_search = f"""
                   SELECT artist_id FROM artists
                   WHERE insert_date = '{insert_date}'
                   """
    result = fetchall_query(QUERY=query_search)

    id_list = [artist[0] for artist in result]

    def do_work(id_list, insert_date, cnt):
        for id in id_list:
            start_time = time()

            endpoint = f'artists/{id}'
            params = {'market' : 'KR'}
            response = get_response(cnt=cnt, endpoint=endpoint, params=params)

            file_dir = f"{data_dir}/artists/{insert_date}/{id}.json"
            try :
                with open(file_dir, "a") as file:
                    json.dump(response, file, indent=4)
            except:
                with open(file_dir, "w") as file:
                    json.dump(response, file, indent=4)

            end_time = time()
            remain_time = 1.5 - (end_time - start_time)
            sleep(remain_time) if remain_time > 0 else sleep(0)

    num_threads = 10
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
    thread_albums(insert_date="2023-12-25")
    