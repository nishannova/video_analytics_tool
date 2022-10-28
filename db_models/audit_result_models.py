from typing import Optional
from sqlmodel import SQLModel, Field, Relationship
from enum import Enum as Enum_, IntEnum
   
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy() 

class VideoProperties(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    resolution = db.Column(db.String)
    quality = db.Column(db.String)
    frame_rate = db.Column(db.Integer)
    distortion_score = db.Column(db.Float)
    watermark = db.Column(db.String)
    aspect_ratio = db.Column(db.Boolean)
    duration = db.Column(db.Float)

# def init_db():
#     db.create_all()

# if __name__ == "__main__":
#     init_db()