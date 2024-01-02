import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.join(current_dir, f'../lib')
data_dir = os.path.join(current_dir, f'../data')
config_dir = os.path.join(current_dir, f'../config')
sys.path.append(lib_dir)

from spotify_libs import *
from postgresql_libs import *

def browse_new_releases():
    from time import time, sleep
    from datetime import datetime

    insert_date = datetime.now().strftime("%Y-%m-%d")
    endpoint = 'browse/new-releases'

    for page in range(0, 20):
        start_time = time()

        params = {
            'limit': '50',
            'offset': page * 50,
        }
        response = get_response(cnt=1, endpoint=endpoint, params=params)

        for item in response["albums"]["items"]:
            album_id = item["id"]
            release_date = item["release_date"]
            query_albums = f'''
                                INSERT INTO albums (album_id, release_date, insert_date)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (album_id) DO NOTHING;
                           '''
            values = (album_id, release_date, insert_date)
            execute_query(query_albums, values)

            for artist in item["artists"]: 
                artist_id = artist["id"]
                query_artists = f'''
                                    INSERT INTO artists (artist_id, insert_date)
                                    VALUES (%s, %s)
                                    ON CONFLICT (artist_id) DO NOTHING;
                                '''
                values = (artist_id, insert_date)
                execute_query(query_artists, values)

        end_time = time()
        remain_time = 2.0 - (end_time - start_time)
        sleep(remain_time) if remain_time > 0 else sleep(0)

def browse_featured_playlists():
    from time import time, sleep
    from datetime import datetime

    start_time = time()

    insert_date = datetime.now().strftime("%Y-%m-%d")
    insert_time = datetime.now().strftime("%H:%M:%S")
    endpoint = 'browse/featured-playlists'
    params = {    
        'country' : 'KR',
        'locale' : 'en_KR',
        'timestamp' : f'{insert_date}T{insert_time}',
        'limit' : 1,
        'offset' : 0
    }
    response = get_response(2, endpoint=endpoint, params=params)

    end_time = time()
    remain_time = 2.0 - (end_time - start_time)
    sleep(remain_time)      

    start_time = time()

    playlist_id = response['playlists']['items'].pop()['id']
    endpoint = f'playlists/{playlist_id}/tracks'
    params = {
        'market' : 'KR',
        'fields' : 'items(track(album(id,release_date),artists(id)))',
        'limit' : 50,
        'offset' : 0
    }
    response = get_response(cnt=2, endpoint=endpoint, params=params)

    for item in response['items'] :
        album_id = item['track']['album']['id']
        release_date = item['track']['album']['release_date']
        query_albums = f'''
                        INSERT INTO albums (album_id, release_date, insert_date)
                        values (%s, %s, %s)
                        ON CONFLICT (album_id) DO NOTHING;
                        '''
        values = (album_id, release_date, insert_date)
        execute_query(query_albums, values)

        for artist in item['track']['artists'] : 
            artist_id = artist['id']
            query_artists = f'''
                                INSERT INTO artists (artist_id, insert_date)
                                VALUES (%s, %s)
                                ON CONFLICT (artist_id) DO NOTHING;
                                '''
            values = (artist_id, insert_date)
            execute_query(query_artists, values)

    end_time = time()
    remain_time = 2.0 - (end_time - start_time)
    sleep(remain_time) if remain_time > 0 else sleep(0)

playlist_ids = []
def thread_playlists_tracks():
    from datetime import datetime
    from threading import Thread
    from time import time, sleep
    from math import ceil
    import json
    
    ids_dir = f"{config_dir}/ids/playlistCategory.json"
    with open(ids_dir, 'r') as file:
        data = json.load(file)
    id_list = data["id_list"]
    
    def update_playlist_ids(id:str):
        global playlist_ids
        playlist_ids.append(id)
    
    def return_ids(id_list_thread:list, cnt:int):
        for category_id in id_list_thread:
            start_time = time()
            
            endpoint = f"browse/categories/{category_id}/playlists?country=KR"
            try:
                response = get_response(cnt=cnt, endpoint=endpoint)
                for playlists in response["playlists"]["items"]:
                    playlist_id = playlists["id"]
                    update_playlist_ids(id=playlist_id)
            except:
                pass
            
            end_time = time()
            remain_time = end_time - start_time
            sleep(remain_time) if end_time - start_time < 2 else sleep(0)
            
    num_threads = 10
    ids_per_thread = ceil(len(id_list) / num_threads)
    thread_list = [id_list[i:i+ids_per_thread] for i in range(0, len(id_list), ids_per_thread)]

    threads = []
    for idx, id_list_thread in enumerate(thread_list):
        thread = Thread(target=return_ids, args=(id_list_thread, idx+1))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    
    def do_work(playlist_ids_thread:list, cnt:int):
        for playlist_id in playlist_ids_thread:
            start_time = time()

            print(f"<<< START ID {playlist_id}")
            endpoint = f'playlists/{playlist_id}/tracks'
            params = {
                'market' : 'KR',
                'fields' : 'items(track(album(id,release_date),artists(id)))',
                'limit' : 50,
                'offset' : 0
            }

            try:
                response = get_response(cnt=cnt, endpoint=endpoint, params=params)

                for item in response['items'] :
                    album_id = item['track']['album']['id']
                    release_date = item['track']['album']['release_date']
                    query_albums = f'''
                                    INSERT INTO albums (album_id, release_date, insert_date)
                                    values (%s, %s, %s)
                                    ON CONFLICT (album_id) DO NOTHING;
                                    '''
                    values = (album_id, release_date, insert_date)
                    execute_query(query_albums, values)

                    for artist in item['track']['artists'] : 
                        artist_id = artist['id']
                        query_artists = f'''
                                            INSERT INTO artists (artist_id, insert_date)
                                            VALUES (%s, %s)
                                            ON CONFLICT (artist_id) DO NOTHING;
                                            '''
                        values = (artist_id, insert_date)
                        execute_query(query_artists, values)
            except:
                pass
                
            end_time = time()
            remain_time = 2.0 - (end_time - start_time)
            sleep(remain_time) if remain_time > 0 else sleep(0)

    insert_date = datetime.now().strftime("%Y-%m-%d")    
    
    num_threads = 10
    length_playlist_ids_thread = ceil(len(playlist_ids) / num_threads)
    playlist_ids_lists = [playlist_ids[i:i+length_playlist_ids_thread] for i in range(0, len(playlist_ids), length_playlist_ids_thread)]

    threads = []
    for idx, id_list_thread in enumerate(playlist_ids_lists):
        thread = Thread(target=do_work, args=(id_list_thread, idx+1))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def thread_artists_related_artists(insert_date):
    from threading import Thread
    from time import time, sleep
    from math import ceil

    query_search = f"""
                   SELECT artist_id FROM artists
                   WHERE insert_date = '{insert_date}'
                   """
    result = fetchall_query(QUERY=query_search)
    id_list = [artist[0] for artist in result]

    def do_work(id_list, insert_date:str, cnt:int):
        for artist_id in id_list:
            start_time = time()

            endpoint = f'artists/{artist_id}/related-artists'
            response = get_response(cnt=cnt, endpoint=endpoint)
            for related_artist in response['artists']:
                related_artist_id = related_artist['id']
                query_artists = f'''
                                INSERT INTO artists (artist_id, insert_date)
                                VALUES (%s, %s)
                                ON CONFLICT (artist_id) DO NOTHING;
                                '''
                values = (related_artist_id, insert_date)
                execute_query(query_artists, values)

            end_time = time()
            remain_time = 2.0 - (end_time - start_time)
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

def thread_artists_albums(insert_date:str):
    from threading import Thread
    from time import time, sleep
    from math import ceil

    query_search = f"""
                   SELECT artist_id FROM artists
                   WHERE insert_date = '{insert_date}'
                   """
    result = fetchall_query(QUERY=query_search)
    id_list = [artist[0] for artist in result]

    def do_work(id_list, insert_date, cnt):
        for artist_id in id_list:
            start_time = time()

            endpoint = f'artists/{artist_id}/albums'
            response = get_response(cnt=cnt, endpoint=endpoint)

            for album in response['items']:
                album_id = album['id']
                release_date = album['release_date']
                query_albums = f'''
                                INSERT INTO albums (album_id, release_date, insert_date)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (album_id) DO NOTHING;
                                '''
                values = (album_id, release_date, insert_date)
                execute_query(query_albums, values)

            end_time = time()
            remain_time = 2.0 - (end_time - start_time)
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
    # TEST
    # browse_new_releases()
    # browse_featured_playlists()
    # thread_update_playlist_datas()
    # thread_artists_related_artists(insert_date="2024-01-02")
    thread_artists_albums(insert_date="2024-01-02")