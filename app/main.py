from fastapi import Depends, FastAPI
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

from app.routers import recipes

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*']
)


app.include_router(recipes.router)


@app.get("/")
async def root():
    return {"message": "Hello recipes search Applications!"}


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)



