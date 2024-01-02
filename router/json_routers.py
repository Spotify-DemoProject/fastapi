from fastapi import APIRouter
from util.json_utils import *

router = APIRouter()

@router.get("/json/albums")
async def get_albums(insert_date:str):
	return thread_albums(insert_date=insert_date)

@router.get("/json/artists")
async def get_artists(insert_date:str):
	return thread_artists(insert_date=insert_date)

@router.get("/json/tracks")
async def get_tracks(insert_date:str):
    return thread_tracks(insert_date=insert_date)
