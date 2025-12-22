from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import router


app = FastAPI(tittle="Sistema RI - Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def hello():
    return {"message": "Hello World from FastAPI!"}


app.include_router(router)
