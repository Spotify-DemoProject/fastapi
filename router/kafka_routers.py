from fastapi import APIRouter
from util.kafka_utils import *

router = APIRouter()

@router.get("/kafka/albums")
async def get_albums(insert_date:str):
	return(publish_message_kafka(insert_date=insert_date, category="albums"))

@router.get("/kafka/artists")
async def get_artists(insert_date:str):
	return(publish_message_kafka(insert_date=insert_date, category="artists"))

@router.get("/kafka/tracks")
async def get_tracks(insert_date:str):
	return(publish_message_kafka(insert_date=insert_date, category="tracks"))
