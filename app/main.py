from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import query, breaches

app = FastAPI(title="Combined Risk & Breaches API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(query.router)
app.include_router(breaches.router)

@app.get("/")
def health_check():
    return {"status": "ok", "routes": [r.path for r in app.routes]}

