from fastapi import APIRouter
from util.kafka_utils import *

router = APIRouter()

@router.get("/parquet/albums")
async def get_albums(insert_date:str):
	return(publish_message_kafka(insert_date=insert_date, category="albums"))

@router.get("/parquet/artists")
async def get_artists(insert_date:str):
	return(publish_message_kafka(insert_date=insert_date, category="artists"))

@router.get("/parquet/tracks")
async def get_artists(insert_date:str):
	return(publish_message_kafka(insert_date=insert_date, category="tracks"))
