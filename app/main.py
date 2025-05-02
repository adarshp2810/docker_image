from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# import both routers
from app.routers.query import router as query_router
from app.routers.breaches import router as breaches_router

app = FastAPI(title="Combined Risk & Breaches API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# mount them both
app.include_router(query_router)
app.include_router(breaches_router)

@app.get("/")
def health_check():
    return {"status": "ok", "routes": [route.path for route in app.routes]}
