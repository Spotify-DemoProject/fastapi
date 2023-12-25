
def get_acccess_token(cnt):
    import os
    from configparser import ConfigParser

    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir = os.path.join(current_dir, f'../config/config.ini')

    parser = ConfigParser()
    parser.read(config_dir)
    access_token = parser.get("SPOTIFY", f"access_token_{cnt}")
    return access_token


def get_response(cnt, endpoint, params:dict=None):
    import os, sys, requests
    from datetime import datetime

    nowdate = datetime.now().strftime("%Y-%m-%d")
    nowtime = datetime.now().strftime("%H:%M:%S")

    current_dir = os.path.dirname(os.path.abspath(__file__))

    access_token = get_acccess_token(cnt)
    url = f"https://api.spotify.com/v1/{endpoint}"
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    if params != None:
        response = requests.get(url=url, params=params, headers=headers)
    else:
        response = requests.get(url=url, headers=headers)

    data = response.json()
    return data

