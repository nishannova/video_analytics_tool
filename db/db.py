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

    name = Column(String, nullable=False, primary_key=True, unique = True)
    video_no = Column(String)
    url = Column(String)
    type = Column(String)
    resolution = Column(String)
    quality = Column(String)
    frame_rate = Column(Integer)
    distortion = Column(Float)
    watermark = Column(String)
    aspect_ratio = Column(Boolean)
    duration = Column(Float)

    def __init__(self, name, video_no="", url="", type="", quality="", resolution="", frame_rate=0, distortion=0.0, aspect_ratio=False, duration=0.0, watermark=""):
        self.name = name
        self.video_no = video_no
        self.url = url
        self.type = type
        self.resolution = resolution
        self.quality = quality
        self.frame_rate = frame_rate
        self.distortion = distortion
        self.watermark = watermark
        self.aspect_ratio = aspect_ratio
        self.duration = duration

base.metadata.create_all(engine)

def persist_audit_result(name, quality, resolution, frame_rate, distortion, aspect_ratio, duration, watermark):
    
    # vid_res = VideoProperties(
        # name=name, 
        # resolution=resolution,
        # quality=quality,
        # frame_rate=frame_rate,
        # distortion=distortion,
        # watermark=watermark,
        # aspect_ratio=aspect_ratio,
        # duration=duration
    #     )
    vid_res = dict()
    vid_res["name"]=name
    vid_res["resolution"]=resolution
    vid_res["quality"]=quality
    vid_res["frame_rate"]=frame_rate
    vid_res["distortion"]=distortion
    vid_res["watermark"]=watermark
    vid_res["aspect_ratio"]=aspect_ratio
    vid_res["duration"]=duration

    vid_record = session.query(VideoProperties).filter(VideoProperties.name == name).first()
    if vid_record:
        session.query(VideoProperties).filter(VideoProperties.name == name).update(vid_res)
        session.commit()
    else:
        session.add(vid_res)
        session.commit()
    
    return True

def persist_initial_record(video_no, url, type, filename):
    vid_res = VideoProperties(
        name = filename,
        video_no = video_no,
        url = url,
        type = type,
        )
    vid_record = session.query(VideoProperties).filter(VideoProperties.name == filename).first()
    if vid_record:
        session.delete(vid_record)
        logger.info(f"Delete old record for filename: {filename}")
     
    session.add(vid_res)
    session.commit()
    return True

def fetch_records(filename, video_no=""):
    if filename and video_no:
        record = session.query(VideoProperties).filter(VideoProperties.name == filename, VideoProperties.video_no == video_no).first()
    elif filename:
        record = session.query(VideoProperties).filter(VideoProperties.name == filename).first()
    elif video_no:
        record = session.query(VideoProperties).filter(VideoProperties.video_no == video_no).first()
    if record:
        response = json.dumps(
            {
            "video_no": record.video_no,
            "url": record.url,
            "type": record.type,
            "name": record.name, 
            "resolution": record.resolution, 
            "frame_rate": record.frame_rate, 
            "distortion_score": record.distortion,
            "watermark": record.watermark, 
            "aspect_ratio": record.aspect_ratio,
            "duration": record.duration
            }
            )
    else:
        return json.dumps(
            {
                "MESSAGE": f"NO RECORDS FOUND FOR ID: {video_no} and FILENAME: {filename}"
            }
        )
    return response

if __name__=="__main__":
    name = "xyz"
    vid_res = VideoProperties(
        name = "xyz",
        video_no = "001",
        url = "www.xyz.com",
        type = "Instagram",
        )
    
    vid_record = session.query(VideoProperties).filter(VideoProperties.name == name).first()
    if vid_record:
        session.delete(vid_record)
        logger.info(f"Delete old record for filename: {name}")
     
    session.add(vid_res)
    session.commit()

    print(f'FIRST FETCH: {fetch_records("xyz")}')

    persist_audit_result(name, "FHD", "1280 x 784", 30, 31.3, True, 23.1, "Instagram test wayermark")

    print(f'SECOND FETCH: {fetch_records("xyz")}')