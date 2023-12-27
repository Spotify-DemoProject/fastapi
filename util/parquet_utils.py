import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.join(current_dir, f'../lib')
config_dir = os.path.join(current_dir, f'../config/config.ini')
sys.path.append(lib_dir)

from s3_libs import *

def upload_parquets(category:str, insert_date:str):
    bucket_name = "spotify-parquet-backup"
    local_dir = f"/home/hooniegit/git/Spotify-DemoProject/spark/data/parquet/{category}/{insert_date}"
    
    upload_to_s3(bucket_name=bucket_name, category=category, local_dir=local_dir)
    
if __name__ == "__main__":
    upload_parquets(category="artists", insert_date="2023-12-25")
    