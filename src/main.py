import sys
sys.path.append("..")
from watermark_detection.watermark import run_watermark
from quality_analysis.quality_analysis import QualityAnalysis
from audio_audit.analysis import process_audio
import cv2
import shutil
import os
from loguru import logger
from watermark_detection.detect import process_detection
import time 
import ray

from threading import Thread
from multiprocessing import Process

from db.db import persist_audit_result, processing_status_update
from config import (
    RAW_DIR, 
    PROCESSED_ORIGINAL_DIR, 
    IN_PROCESS_DIR, 
    FAILED_DIR,
    TEMP_FRAMES_DIR
)

# RAW_DIR = '/home/saintadmin/work/video_analytics_tool/data/raw'
# PROCESSED_ORIGINAL_DIR = "/home/saintadmin/work/video_analytics_tool/data/processed/original_video"
# IN_PROCESS_DIR = '/home/saintadmin/work/video_analytics_tool/data/in_process'
# FAILED_DIR = "/home/saintadmin/work/video_analytics_tool/data/failed_files"

class AuditVideo():
    def __init__(self, file_name):
        self.file_name = file_name
        self.quality_details = dict()
        self.quality_details["QUALITY_ANALYSIS"] = {}
        self.quality_details["DETECTED WATERMARKS"] = {}
        self.quality_details["SANITY CHECKS"] = {}
        self.quality_details["AUDIO AUDIT"] = {}
        if processing_status_update(self.file_name, "Processing"):
            logger.info(f"PROCESING STATUS UPDATED IN DB FOR: {file_name}")
        
    def fit(self):
        try:
            if not self.file_name.endswith("png"):
                logger.info(f"INITIALIZING AUDIT PROCESS FOR: {self.file_name}")
                vid_q, resolution, frame_rate, distortion, aspect_ratio, duration, watermark = None, None, None, None, None, None, None
                height, width = 0, 0
                em_sadness, em_others, em_fear, em_disgust, em_surprise, em_joy, em_anger = "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A"
                ht_hateful, ht_targeted, ht_aggressive = "N/A", "N/A", "N/A"
                #QUALITY OBJECTS
                q_obj = QualityAnalysis(cv2.VideoCapture(os.path.join(IN_PROCESS_DIR, self.file_name)), self.file_name)
                q_obj.split_frames(cv2.VideoCapture(os.path.join(IN_PROCESS_DIR, self.file_name)))
                vid_q, resolution, height, width = q_obj.resolution_analysis(cv2.VideoCapture(os.path.join(IN_PROCESS_DIR, self.file_name)))
                frame_rate = int(q_obj.frame_rate_analysis(cv2.VideoCapture(os.path.join(IN_PROCESS_DIR, self.file_name))))
                distortion = round(q_obj.distortion_analysys(), 2)
                aspect_ratio = q_obj.aspect_ratio_analysis(cv2.VideoCapture(os.path.join(IN_PROCESS_DIR, self.file_name)))
                duration = q_obj.duration(cv2.VideoCapture(os.path.join(IN_PROCESS_DIR, self.file_name)))
                #QUALITY OBJECTS
                
                #WATERMARK MODULE
                watermark = process_detection(self.file_name)
                watermark = str(watermark) if watermark else "No Watermarks Found"
                #WATERMARK MODULE

                #AUDIO MODULE
                emotion, hate_speech = process_audio(os.path.join(IN_PROCESS_DIR, self.file_name))
                #AUDIO MODULE

                # if emotion:
                em_sadness = str(emotion.get("sadness", "N/A"))
                em_others = str(emotion.get("others", "N/A"))
                em_fear = str(emotion.get("fear", "N/A"))
                em_disgust = str(emotion.get("disgust", "N/A"))
                em_surprise = str(emotion.get("surprise", "N/A"))
                em_joy = str(emotion.get("joy", "N/A"))
                em_anger = str(emotion.get("anger", "N/A"))
                # if hate_speech:
                ht_hateful = str(hate_speech.get("hateful", "N/A"))
                ht_targeted = str(hate_speech.get("targeted", "N/A"))
                ht_aggressive = str(hate_speech.get("aggressive", "N/A"))

                self.quality_details["QUALITY_ANALYSIS"]["RESOLUTION"] = f"Video type: [{vid_q}] and Resolution: {resolution}"
                self.quality_details["QUALITY_ANALYSIS"]["FRAME RATE"] = str(frame_rate) + " FPS"
                self.quality_details["QUALITY_ANALYSIS"]["DISTORTION SCORE"] = str(distortion) + " %"
                self.quality_details["SANITY CHECKS"]["ACCEPTABLE ASPECT RATIO"] = str(aspect_ratio)
                self.quality_details["SANITY CHECKS"]["DURATION"] = str(duration) + " Sec"
                self.quality_details["DETECTED WATERMARKS"]["CONTENTS"] = watermark.split(",")
                self.quality_details["AUDIO AUDIT"]["EMOTION_DETECTION"] = emotion
                self.quality_details["AUDIO AUDIT"]["HATE_SPEECH_DETECTION"] = hate_speech

                logger.info(self.quality_details)
                
                persist_flag = persist_audit_result(
                                                    self.file_name, 
                                                    vid_q, 
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
                                                    "Completed"
                                                        )
                if persist_flag:
                    logger.info("Successfullt Persisted Records")
                

                # remove_artifacts(self.file_name)
                # logger.info("Successfully removed all the temporary artifacts: {self.file_name}")
            try:
                shutil.move(os.path.join(IN_PROCESS_DIR, self.file_name), os.path.join(PROCESSED_ORIGINAL_DIR, self.file_name))
                logger.warning(f"MOVED FILE: {self.file_name} TO PROCESSED ORIGINAL")
                self.remove_artifacts()
                logger.info(f"Successfully removed all the temporary artifacts: {self.file_name}")
            except Exception as ex:
                logger.error(f"ERROR: [ {ex} ] WHILE MOVING FILES TO ORIGINAL")
        except Exception as ex:
            logger.error(f"ERROR: {ex} WHILE PROCESSING VIDEO: {self.file_name}")
            if processing_status_update(self.file_name, "Error"):
                logger.info(f"PROCESING STATUS UPDATED IN DB FOR: {self.file_name}")
            try:
                self.remove_artifacts()
                logger.info(f"Successfully removed all the temporary artifacts: {self.file_name}")
            except Exception as ex:
                logger.error(f"ERROR: [ {ex} ] WHILE MOVING FILES TO ORIGINAL")

    def remove_artifacts(self):
        try:
            shutil.rmtree(os.path.join(TEMP_FRAMES_DIR,self.file_name))
            os.remove(os.path.join(PROCESSED_ORIGINAL_DIR, self.file_name)) 
        except Exception as ex:
            logger.error(f"FAILED TO REMOVE ARTIFACTS: {self.file_name} with {ex}")


    def detect_watermark(self, cam=False):
        run_watermark(self.vs, cam)

    def analyse_video_quality(self):
        pass

if __name__ == "__main__":
    # quality_details = dict()
    # quality_details["QUALITY_ANALYSIS"] = {}
    # quality_details["DETECTED WATERMARKS"] = {}
    # quality_details["SANITY CHECKS"] = {}
    count = 0

    if os.listdir(IN_PROCESS_DIR):
        logger.warning(f"FOUND FAILED FILES IN IN-PROCESS. MOVING THEM TO RAW")
        for file in os.listdir(IN_PROCESS_DIR):
            shutil.move(os.path.join(IN_PROCESS_DIR, file), os.path.join(RAW_DIR, file))
            logger.warning(f"MOVED: {file} FROM IN-PROCESS TO RAW")    
    cycle = 1 
    while True:
        start = time.time()
        # logger.info(f"*****************ML PIPELINE BEGINING*******************")
        raw_files = os.listdir(RAW_DIR)
        path = os.listdir(IN_PROCESS_DIR)

        if not raw_files:
            # logger.warning(f"NO FILES IN LANDING DIRECTORY CHECKING AFTER A QUICK NAP :)")
            # ray.shutdown()
            # logger.warning(f"SHUTTING DOWN DISTRIBUTION ENGINE")
            time.sleep(3)
            continue
        logger.info(f"FOUND LISTED FILES IN RAW:\n{raw_files}")
        
        for file in raw_files:
            shutil.move(os.path.join(RAW_DIR, file), os.path.join(IN_PROCESS_DIR, file))
            logger.warning(f"MOVED: {file} FROM RAW TO IN-PROCESS")
            count=count+1
            if count==2:
                count=0
                break
        
        objects = [AuditVideo(file_name) for file_name in os.listdir(IN_PROCESS_DIR) if not file_name.endswith("png")]
        logger.info(f"OBJECTS: {objects}")
        # threads = [Thread(target=obj.fit) for obj in objects]
        processes = [Process(target=obj.fit) for obj in objects]
        logger.info(f"PROCESSES: {processes}")
        _ = [thread.start() for thread in processes]
        _ = [thread.join() for thread in processes]
        logger.info(f"COMPLETED {cycle} CYCLE")
        cycle += 1
        logger.info(f"ENTIRE PROCESSING CYCLE TOOK: {time.time() - start} Secs")
        failed_files = os.listdir(IN_PROCESS_DIR)
        ray.shutdown()
        if failed_files:
            for file in failed_files:
                logger.warning(f"MOVING: {file} to FAILED FILE DIRECTORY")
                try:
                    shutil.move(os.path.join(IN_PROCESS_DIR, file), os.path.join(FAILED_DIR, file))
                except Exception as ex:
                    logger.error(f"FAILED TO MOVE: {file} WITH ERROR: {ex}")
    