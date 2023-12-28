from fastapi import APIRouter
from util.parquet_utils import *

router = APIRouter()

@router.get("/parquet/albums")
async def get_albums(insert_date:str):
	return(upload_parquets(insert_date=insert_date, category="albums"))

@router.get("/parquet/artists")
async def get_artists(insert_date:str):
	return(upload_parquets(insert_date=insert_date, category="artists"))

@router.get("/parquet/tracks")
async def get_artists(insert_date:str):
	return(upload_parquets(insert_date=insert_date, category="tracks"))
