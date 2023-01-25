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
    em_sadness = Column(String)
    em_others = Column(String)
    em_fear = Column(String)
    em_disgust = Column(String)
    em_surprise = Column(String)
    em_joy = Column(String)
    em_anger = Column(String)
    ht_hateful = Column(String)
    ht_targeted = Column(String)
    ht_aggressive = Column(String)
    height = Column(Integer)
    width = Column(Integer)
    status = Column(String)

    def __init__(
                    self, 
                    name, 
                    video_no="", 
                    url="", 
                    type="", 
                    quality="", 
                    resolution="", 
                    frame_rate=0, 
                    distortion=0.0, 
                    aspect_ratio=False, 
                    duration=0.0, 
                    watermark="",
                    em_sadness="N/A", 
                    em_others="N/A", 
                    em_fear="N/A", 
                    em_disgust="N/A", 
                    em_surprise="N/A", 
                    em_joy="N/A", 
                    em_anger="N/A",
                    ht_hateful="N/A", 
                    ht_targeted="N/A", 
                    ht_aggressive="N/A",
                    height = 0,
                    width = 0,
                    status = "N/A"
                    ):
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
        self.em_sadness = em_sadness
        self.em_others = em_others
        self.em_fear = em_fear
        self.em_disgust = em_disgust
        self.em_surprise = em_surprise
        self.em_joy = em_joy
        self.em_anger = em_anger
        self.ht_hateful = ht_hateful
        self.ht_targeted = ht_targeted
        self.ht_aggressive = ht_aggressive
        self.height = height
        self.width = width
        self.status = status

base.metadata.create_all(engine)

def processing_status_update(name, status="In-process"):
    vid_res = dict()
    vid_res["name"]=name
    vid_res["status"]=status
    
    try:
        vid_record = session.query(VideoProperties).filter(VideoProperties.name == name).first()
        if vid_record:
            vid_record.status = status
            # session.query(VideoProperties).filter(VideoProperties.name == name).update(vid_res)
            session.commit()
        else:
            # session.add(vid_res)
            # session.commit()
            logger.error(f"NO RECORDS FOUND FOR:  {name}")
        
        return True
    except Exception as ex:
        logger.error(f"Error caught up in processing status update")
        return False

def persist_audit_result(
    name, 
    quality, 
    resolution, 
    frame_rate, 
    distortion, 
    aspect_ratio, 
    duration, 
    watermark,
    em_sadness, 
    em_others, 
    em_fear, 
    em_disgust, 
    em_surprise, 
    em_joy, 
    em_anger,
    ht_hateful, 
    ht_targeted, 
    ht_aggressive,
    height, 
    width,
    status
    ):
    
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
    try:
        vid_res["name"]=name
        if resolution:
            vid_res["resolution"]=resolution 
        if quality:
            vid_res["quality"]= quality 
        if frame_rate:
            vid_res["frame_rate"]=frame_rate
        if distortion:
            vid_res["distortion"]=distortion
        if watermark:
            vid_res["watermark"]=watermark 
        if aspect_ratio:
            vid_res["aspect_ratio"]=aspect_ratio
        if duration:
            vid_res["duration"]=duration 
        if em_sadness:
            vid_res["em_sadness"]=em_sadness 
        if em_others:
            vid_res["em_others"]=em_others
        if em_fear:
            vid_res["em_fear"]=em_fear 
        if em_disgust:
            vid_res["em_disgust"]=em_disgust 
        if em_surprise:
            vid_res["em_surprise"]=em_surprise 
        if em_joy:
            vid_res["em_joy"]=em_joy 
        if em_anger:
            vid_res["em_anger"]=em_anger 
        if ht_hateful:
            vid_res["ht_hateful"]=ht_hateful 
        if ht_targeted:
            vid_res["ht_targeted"]=ht_targeted 
        if ht_aggressive:
            vid_res["ht_aggressive"]=ht_aggressive 
        if height:
            vid_res["height"]=height 
        if width:
            vid_res["width"]=width 
        if status:
            vid_res["status"]=status 
        
        
        vid_record = session.query(VideoProperties).filter(VideoProperties.name == name).first()
        if vid_record:
            logger.info(f"INITIAL RECORD FOUND FOR: {name}, UPDATING DETAILS")
            session.query(VideoProperties).filter(VideoProperties.name == name).update(vid_res)
            session.commit()
        else:
            logger.error(f"INITIAL RECORD NOT CREATED FOR: {name}")
            # session.add(vid_res)
            # columns = list(vid_res.keys())
            return False
            # session.commit()
        
        return True
    except Exception as ex:
        logger.error(f"Error caught up: {ex} in persisting")
        return False

def persist_initial_record(video_no, url, type, filename, status):
    try:
        vid_res = VideoProperties(
            name = filename,
            video_no = video_no,
            url = url,
            type = type,
            status = status
            )
        vid_record = session.query(VideoProperties).filter(VideoProperties.name == filename).first()
        if vid_record:
            session.delete(vid_record)
            logger.info(f"Delete old record for filename: {filename}")
        
        if video_no:
            vid_record_1 = session.query(VideoProperties).filter(VideoProperties.video_no == video_no).first()
            if vid_record_1:
                session.delete(vid_record_1)
                logger.info(f"Delete old record for video_no: {video_no}")

        session.add(vid_res)
        session.commit()
        return True
    except Exception as ex:
        logger.error(f"Error caught up: {ex} in persisting initial record")
        return False

def fetch_records(filename, video_no=""):
    try:
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
                "duration": record.duration,
                "em_sadness": record.em_sadness, 
                "em_others": record.em_others, 
                "em_fear": record.em_fear, 
                "em_disgust": record.em_disgust, 
                "em_surprise": record.em_surprise, 
                "em_joy": record.em_joy, 
                "em_anger": record.em_anger,
                "ht_hateful": record.ht_hateful, 
                "ht_targeted": record.ht_targeted, 
                "ht_aggressive": record.ht_aggressive,
                "height": record.height,
                "width": record.width,
                "status": record.status
                }
                )
        else:
            return json.dumps(
                {
                    "MESSAGE": f"NO RECORDS FOUND FOR ID: {video_no} and FILENAME: {filename}"
                }
            )
        return response
    except Exception as ex:
        logger.error(f"Error caught up: {ex} in extracting record")
        return False
        
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