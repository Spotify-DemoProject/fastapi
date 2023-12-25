from fastapi import APIRouter

router = APIRouter()

@router.get("/csv/albums")
async def get_albums(insert_date:str):
	pass

@router.get("/csv/artists")
async def get_artists(insert_date:str):
	pass