import sys
sys.path.append("..")
from watermark_detection.watermark import run_watermark
from quality_analysis.quality_analysis import QualityAnalysis

import cv2
import shutil
import os
from loguru import logger
from watermark_detection.detect import process_detection
import time 
import ray
from threading import Thread

from db.db import persist_audit_result

source = '/home/saintadmin/work/video_analytics_tool/data/raw'
original_path = "/home/saintadmin/work/video_analytics_tool/data/processed/original_video"
destination = '/home/saintadmin/work/video_analytics_tool/data/in_process'


class AuditVideo():
    def __init__(self, file_name):
        self.file_name = file_name
    
    def fit(self):
        if not self.file_name.endswith("png"):
            logger.info(f"INITIALIZING AUDIT PROCESS FOR: {self.file_name}")
            vid_q, resolution, frame_rate, distortion, aspect_ratio, duration, watermark = None, None, None, None, None, None, None
            q_obj = QualityAnalysis(cv2.VideoCapture(os.path.join(destination, self.file_name)), self.file_name)
            q_obj.split_frames(cv2.VideoCapture(os.path.join(destination, self.file_name)))
            vid_q, resolution = q_obj.resolution_analysis(cv2.VideoCapture(os.path.join(destination, self.file_name)))
            frame_rate = int(q_obj.frame_rate_analysis(cv2.VideoCapture(os.path.join(destination, self.file_name))))
            distortion = round(q_obj.distortion_analysys(), 2)
            aspect_ratio = q_obj.aspect_ratio_analysis(cv2.VideoCapture(os.path.join(destination, self.file_name)))
            duration = q_obj.duration(cv2.VideoCapture(os.path.join(destination, self.file_name)))
            watermark = process_detection(self.file_name)
            watermark = str(watermark) if watermark else "No Watermarks Found"

            quality_details["QUALITY_ANALYSIS"]["RESOLUTION"] = f"Video type: [{vid_q}] and Resolution: {resolution}"
            quality_details["QUALITY_ANALYSIS"]["FRAME RATE"] = str(frame_rate) + " FPS"
            quality_details["QUALITY_ANALYSIS"]["DISTORTION SCORE"] = str(distortion) + " %"
            quality_details["SANITY CHECKS"]["ACCEPTABLE ASPECT RATIO"] = str(aspect_ratio)
            quality_details["SANITY CHECKS"]["DURATION"] = str(duration) + " Sec"
            quality_details["DETECTED WATERMARKS"]["CONTENTS"] = watermark.split(",")


            logger.info(quality_details)
            
            persist_flag = persist_audit_result(self.file_name, vid_q, resolution, frame_rate, distortion, aspect_ratio, duration, watermark)
            if persist_flag:
                logger.info("Successfullt Persisted Records")
            # remove_artifacts(self.file_name)
            # logger.info("Successfully removed all the temporary artifacts: {self.file_name}")
        try:
            shutil.move(os.path.join(destination, self.file_name), os.path.join(original_path, self.file_name))
            logger.warning(f"MOVED FILE: {self.file_name} TO PROCESSED ORIGINAL")
        except Exception as ex:
            logger.error(f"ERROR: [ {ex} ] WHILE MOVING FILES TO ORIGINAL")
        


    def detect_watermark(self, cam=False):
        run_watermark(self.vs, cam)

    def analyse_video_quality(self):
        pass

if __name__ == "__main__":
    start = time.time()
    quality_details = dict()
    quality_details["QUALITY_ANALYSIS"] = {}
    quality_details["DETECTED WATERMARKS"] = {}
    quality_details["SANITY CHECKS"] = {}
    count = 0

    if os.listdir(destination):
        logger.warning(f"FOUND FAILED FILES IN IN-PROCESS. MOVING THEM TO RAW")
        for file in os.listdir(destination):
            shutil.move(os.path.join(destination, file), os.path.join(source, file))
            logger.warning(f"MOVED: {file} FROM IN-PROCESS TO RAW")    
    cycle = 1 
    while True:
        logger.info(f"*****************ML PIPELINE BEGINING*******************")
        raw_files = os.listdir(source)
        path = os.listdir(destination)

        if not raw_files:
            logger.warning(f"NO FILES IN LANDING DIRECTORY CHECKING AFTER A QUICK NAP :)")
            ray.shutdown()
            logger.warning(f"SHUTTING DOWN DISTRIBUTION ENGINE")
            time.sleep(30)
            continue
        logger.info(f"FOUND LISTED FILES IN RAW:\n{raw_files}")
        
        for file in raw_files:
            shutil.move(os.path.join(source, file), os.path.join(destination, file))
            logger.warning(f"MOVED: {file} FROM RAW TO IN-PROCESS")
            count=count+1
            if count==2:
                count=0
                break

        # for file_name in os.listdir(destination):
        #     if file_name.endswith("png"):
        #         continue
        
        objects = [AuditVideo(file_name) for file_name in os.listdir(destination) if not file_name.endswith("png")]
        logger.info(f"OBJECTS: {objects}")
        threads = [Thread(target=obj.fit) for obj in objects]
        logger.info(f"THREADS: {threads}")
        _ = [thread.start() for thread in threads]
        _ = [thread.join() for thread in threads]
        logger.info(f"COMPLETED {cycle} CYCLE")
        cycle += 1
        
    logger.info(f"ENTIRE PROCESSING TOOK: {time.time() - start} Secs")