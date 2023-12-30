import os, sys

lib_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(lib_dir)

from os_libs import *

def get_acccess_token(cnt):
    from configparser import ConfigParser

    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir = os.path.join(current_dir, f'../config/config.ini')

    parser = ConfigParser()
    parser.read(config_dir)
    access_token = parser.get("SPOTIFY", f"access_token_{cnt}")
    return access_token


def get_response(cnt, endpoint, params:dict=None):
    import requests, json

    access_token = get_acccess_token(cnt)
    url = f"https://api.spotify.com/v1/{endpoint}"
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    if params != None:
        response = requests.get(url=url, params=params, headers=headers)
    else:
        response = requests.get(url=url, headers=headers)
        
    if response.status_code == 200:
        try:
            data = response.json()
            return data
        except json.decoder.JSONDecodeError:
            raise ValueError(f"API Server Error - {endpoint} - Invalid JSON content in response: {response.text}")
    else:
        raise ValueError(f"API Server Error - {endpoint} - Non-200 status code received: {response.status_code}")
