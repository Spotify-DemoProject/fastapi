# fastapi
Spotify 데이터 파이프라인에서 ETL 작업을 수행합니다.<br><br>

# Structure
<img width="772" alt="스크린샷 2024-01-01 오전 1 33 22" src="https://github.com/Spotify-DemoProject/fastapi/assets/130134750/7884869b-5e3c-47c5-82ad-56f92b7dc280">
<br>

# Before Start
``` bash
# install FastAPI
pip install fastapi "uvicorn[standard]" requests

# run FastAPI
nohup python3 main.py &
```
<br>

# Router Endpoints
``` bash
# Postgres 서버에 API 파라미터 적재
/sql/new_release
/sql/featured_playlists
/sql/related_artists
/sql/artist_albums

# .json 데이터 수집
/json/albums
/json/artists

# Kafka 메세지 발행 -> Spark Streaming 앱 스케줄링
/kafka/albums
/kafka/artists

# .parquet 파일 백업 (AWS S3)
/parquet/albums
/parquet/artists
```
<br>

# Results
<img width="1322" alt="스크린샷 2023-12-30 오후 9 40 59" src="https://github.com/Spotify-DemoProject/fastapi/assets/130134750/9d033650-0a2e-4319-9958-83596a207b52">

