from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers.query import router as query_router

app = FastAPI(title="Combined Risk API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(query_router)

@app.get("/")
def health_check():
    return {"status": "ok", "routes": [route.path for route in app.routes]}
