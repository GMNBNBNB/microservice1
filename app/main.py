# main.py
from fastapi import Depends, FastAPI, Request
import uvicorn
import logging
import json
import os

from fastapi.middleware.cors import CORSMiddleware

from app.routers import recipes
from app.correlation_id_middleware import CorrelationIdMiddleware
from app.log_requests_middleware import LogRequestsMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# 添加 CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#add middleware
app.add_middleware(LogRequestsMiddleware)

app.add_middleware(CorrelationIdMiddleware)

app.include_router(recipes.router)

@app.get("/")
async def root(request: Request):
    correlation_id = getattr(request.state, 'correlation_id', 'N/A')
    return {"message": "Hello recipes search Applications!", "correlationId": correlation_id}


# output openAPI file
# with open("openapi.json", "w") as f:
#     json.dump(app.openapi(), f)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
