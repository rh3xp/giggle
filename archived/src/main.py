from fastapi import FastAPI, Depends, Request
from sqlalchemy.orm import Session
import models
from database import SessionLocal, engine
from fastapi.staticfiles import StaticFiles
from datetime import datetime

# Initialize the database
models.Base.metadata.create_all(bind=engine)

# Initialize FastAPI app
app = FastAPI()


# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Mount the static files directory
app.mount("/static", StaticFiles(directory="static"), name="static")


# Route for the homepage
@app.get("/")
async def read_root(request: Request, db: Session = Depends(get_db)):
    # Log the access time
    timestamp = models.Timestamp()
    db.add(timestamp)
    db.commit()
    db.refresh(timestamp)

    # Serve the Three.js HTML file
    return {
        "message": "Visit /static/index.html to see the 3D notepad.",
        "timestamp_id": timestamp.id,
        "accessed_at": timestamp.accessed_at
    }
