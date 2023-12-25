# fastapi
Download Raw JSON Files

# before start
```
pip install fastapi "uvicorn[standard]" requests
```

# router endpoints
```
/sql/new_release
/sql/featured_playlists
/sql/related_artists
/sql/artist_albums
/json/albums
/json/artists
```

# airflow schedule
- new_release 
-> featured_playlists 
-> related_artists 
-> artist_albums 
-> albums 
-> artists
