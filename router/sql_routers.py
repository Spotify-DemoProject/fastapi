from fastapi import APIRouter
from util.sql_utils import *

router = APIRouter()

@router.get("/sql/new_release")
async def get_browse_new_releases():
	return browse_new_releases()

@router.get("/sql/featured_playlists")
async def get_browse_featured_playlists():
	return browse_featured_playlists()

@router.get("/sql/playlists_tracks")
async def get_playlists_tracks():
    return thread_playlists_tracks()

@router.get("/sql/related_artists")
async def get_artists_related_artists(insert_date:str):
	return thread_artists_related_artists(insert_date)

@router.get("/sql/artists_albums")
async def get_artists_albums(insert_date:str):
	return thread_artists_albums(insert_date)
