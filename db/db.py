from loguru import logger 
import json

from sqlalchemy import (create_engine, 
                        MetaData, 
                        Table, 
                        Column, 
                        Integer, 
                        String, 
                        Float, 
                        Boolean
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite://///home/saintadmin/work/video_analytics_tool/db/sample.db', echo=True)
base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


class VideoProperties(base):

    __tablename__ = "videoproperties"


    name = Column(String, nullable=False, primary_key=True)
    resolution = Column(String)
    quality = Column(String)
    frame_rate = Column(Integer)
    distortion = Column(Float)
    watermark = Column(String)
    aspect_ratio = Column(Boolean)
    duration = Column(Float)

    def __init__(self, name, quality="", resolution="", frame_rate=0, distortion=0.0, aspect_ratio=False, duration=0.0, watermark=""):
        self.name = name
        self.resolution = resolution
        self.quality = quality
        self.frame_rate = frame_rate
        self.distortion = distortion
        self.watermark = watermark
        self.aspect_ratio = aspect_ratio
        self.duration = duration

base.metadata.create_all(engine)

def persist_audit_result(name, quality, resolution, frame_rate, distortion, aspect_ratio, duration, watermark):
    
    vid_res = VideoProperties(
        name=name, 
        resolution=resolution,
        quality=quality,
        frame_rate=frame_rate,
        distortion=distortion,
        watermark=watermark,
        aspect_ratio=aspect_ratio,
        duration=duration
        )
    
    vid_record = session.query(VideoProperties).filter(VideoProperties.name == name).first()
    if vid_record:
        session.delete(vid_record)
        logger.info(f"Delete old record for filename: {name}")
     
    session.add(vid_res)
    session.commit()
    
    return True

def fetch_records(filename):
    record = session.query(VideoProperties).filter(VideoProperties.name == filename).first()
    response = json.dumps(
        {
        "name": record.name, 
        "resolution": record.resolution, 
        "frame_rate": record.frame_rate, 
        "distortion_score": record.distortion,
        "watermark": record.watermark, 
        "aspect_ratio": record.aspect_ratio,
        "duration": record.duration
        }
        )
    return response

if __name__=="__main__":
    print(fetch_records("C031-01[1].mp4"))