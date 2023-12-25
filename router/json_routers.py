from fastapi import APIRouter

router = APIRouter()

@router.get("/json/albums")
async def get_albums(insert_date:str):
	pass

@router.get("/json/artists")
async def get_artists(insert_date:str):
	pass
