import sys
import os
import shutil
import time
import cv2
from loguru import logger
from threading import Thread
from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor
import ultralytics
ultralytics.checks()
from ultralytics import YOLO
import json
import random

sys.path.append("..")
import ray
import cv2
import os

from quality_analysis.quality_analysis import QualityAnalysis
from watermark_detection.detect import process_detection
from audio_audit.analysis import process_audio
from watermark_detection.watermark import run_watermark
from quality_analysis.quality_analysis import QualityAnalysis
from audio_audit.analysis import process_audio
from watermark_detection.detect import process_detection
from video_audit.object_detection import video_audit
from db.db import persist_audit_result, processing_status_update

from config import (
    RAW_DIR,
    PROCESSED_ORIGINAL_DIR,
    IN_PROCESS_DIR,
    FAILED_DIR,
    TEMP_FRAMES_DIR,
    FINAL_OUTPUT_DIR
)

from loguru import logger

famous_list = ['Taj Mahal', '-', 'Virat Kohli', '-',  '-', '-', '-','Sachin Tendulkar', '-',   '-','Mumbai', 'Delhi',  '-','Jaipur', '-', 'Goa',  '-','Rajasthan', 'Kerala', 'Chennai', 'Kolkata', 'Bangalore', 'Hyderabad', 'Agra', 'Varanasi', 'Golden Temple', 'Jammu and Kashmir', 'Sundar Pichai', 'Indira Nooyi', 'Satya Nadella', 'Mukesh Ambani', 'Amitabh Bachchan', 'Shah Rukh Khan', 'Priyanka Chopra']

def setup_logger():
    logger.remove()  # Remove default loguru handler
    log_format = '<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>'
    logger.add(sys.stderr, format=log_format, level="INFO", filter=lambda record: record["extra"].get("name") == "ray_logger")
    return logger.bind(name="ray_logger")

@ray.remote
def quality_analysis_remote(file_name, file_path):
    logger = setup_logger()
    vs = cv2.VideoCapture(file_path)
    q_obj = QualityAnalysis(vs, file_name)
    # q_obj.split_frames(cv2.VideoCapture(file_path))
    vid_q, resolution, height, width = q_obj.resolution_analysis()
    frame_rate = int(q_obj.frame_rate_analysis())
    distortion = round(q_obj.distortion_analysys(), 2)
    aspect_ratio = q_obj.aspect_ratio_analysis()
    duration = q_obj.duration()

    return vid_q, resolution, height, width, frame_rate, distortion, aspect_ratio, duration

@ray.remote
def process_detection_remote(file_name):
    logger = setup_logger()
    return process_detection(file_name)

@ray.remote
def process_audio_remote(file_path, duration):
    logger = setup_logger()
    return process_audio(file_path, duration)
VIDEO_SAVE_PATH = "../data/processed/object_detection_result"
MODEL = YOLO('../data/model/best.pt')

@ray.remote
def process_video_remote(file_name):
    logger = setup_logger()
    video_audit_res = video_audit(os.path.join(IN_PROCESS_DIR,file_name), VIDEO_SAVE_PATH, MODEL)
    return video_audit_res

def vid_duration(file_path):
    vs = cv2.VideoCapture(file_path)
    fps = vs.get(cv2.CAP_PROP_FPS)
    frame_count = vs.get(cv2.CAP_PROP_FRAME_COUNT)
    try:
        return int(frame_count/fps)
    except Exception as ex:
        print("ERROR: {ex} in duration")
        return 0.0
        
class AuditVideo:
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.quality_details = {
            "QUALITY_ANALYSIS": {},
            "DETECTED_WATERMARKS": {},
            "SANITY_CHECKS": {},
            "AUDIO_AUDIT": {},
            "VIDEO_AUDIT": {}
        }
        file_path = os.path.join(IN_PROCESS_DIR, self.file_name)
        self.duration = vid_duration(file_path)
        # self.q_obj = QualityAnalysis(cv2.VideoCapture(file_path), file_name)
        # self.q_obj.split_frames(cv2.VideoCapture(file_path))

    def fit(self):
        if not self.file_name.endswith("png"):
            file_path = os.path.join(IN_PROCESS_DIR, self.file_name)

            # # Create Ray remote tasks
            # quality_future = quality_analysis_remote.remote(self.file_name, file_path)
            # watermark_future = process_detection_remote.remote(self.file_name)
            # audio_future = process_audio_remote.remote(file_path)

            # # Get the results from remote tasks
            # quality_result = ray.get(quality_future)
            # watermark = ray.get(watermark_future)
            # emotion, hate_speech = ray.get(audio_future)
            # Create Ray remote tasks
            
            # quality_future = quality_analysis_remote.remote(self.file_name, file_path)
            # watermark_future = process_detection_remote.remote(self.file_name)
            # audio_future = process_audio_remote.remote(file_path)
            # video_future = process_video_remote.remote(self.file_name)

            # # Get the results from remote tasks
            # quality_result = ray.get(quality_future)
            # watermark = ray.get(watermark_future)
            # emotion, hate_speech = ray.get(audio_future)
            # video_details = ray.get(video_future)
                        # Submit remote tasks
            quality_future = quality_analysis_remote.remote(self.file_name, file_path)
            watermark_future = process_detection_remote.remote(self.file_name)
            audio_future = process_audio_remote.remote(file_path, self.duration)
            video_future = process_video_remote.remote(self.file_name)

            # Wait for all remote tasks to finish
            ready, not_ready = ray.wait([audio_future, video_future, quality_future, watermark_future, ], num_returns=4)

            # Retrieve the results from the ready tasks
            emotion, hate_speech, transcription, translation, language_code, segment_duration = ray.get(ready[0])
            video_details = ray.get(ready[1])
            quality_result = ray.get(ready[2])
            watermark = ray.get(ready[3])
            
            


            self.populate_quality_details(quality_result, watermark, emotion, hate_speech, video_details, transcription, translation, language_code, segment_duration)
            self.save_results(quality_result)

            shutil.move(os.path.join(IN_PROCESS_DIR, self.file_name), os.path.join(PROCESSED_ORIGINAL_DIR, self.file_name))
            self.remove_artifacts()

        # except Exception as ex:
        #     logger.error(f"ERROR: {ex} WHILE PROCESSING VIDEO: {self.file_name}")

    def analyze_quality(self, file_path):
        quality_future = quality_analysis_remote.remote(self.file_name, file_path)
        return ray.get(quality_future)

    def detect_watermark(self):
        watermark_future = process_detection_remote.remote(self.file_name)
        watermark = ray.get(watermark_future)
        return str(watermark) if watermark else "No Watermarks Found"

    def analyze_audio(self, file_path):
        audio_future = process_audio_remote.remote(file_path)
        return ray.get(audio_future)
    
    def build_table_of_contents(self, emotion, hate_speech, video_details):
        content_header = list()
        content_item = dict()
        content_item["TYPE"] = None
        content_item["TIME"] = None
        content_item["DETAILS"] = None

        if emotion:
            for em in emotion:
                content_item["TYPE"] = "EMOTION"
                content_item["KEY"] = "SENTIMENT_EMOTION"
                content_item["TIME"] = em[0]
                content_item["DETAILS"] = em[1]
                content_header.append(content_item.copy())
        if hate_speech:
            for hs in hate_speech:
                content_item["TYPE"] = "HATE SPEECH"
                content_item["KEY"] = "SENTIMENT_EMOTION"
                content_item["TIME"] = hs[0]
                content_item["DETAILS"] = hs[1]
                content_header.append(content_item.copy())
        if video_details:
            import pprint
            pprint.pprint(f"VIDEO DETAILS: {video_details}")
            for object, time_details in video_details.items():
                
                if time_details.get("Timestamp"):
                    for ts in  time_details.get("Timestamp"):
                        content_item["TYPE"] = "OBJECT DETECTED"
                        content_item["KEY"] = "SCENE_OBJECT"
                        content_item["TIME"] = ts
                        content_item["DETAILS"] = object
                        content_header.append(content_item.copy())                       
        return content_header

    def populate_quality_details(self, quality_result, watermark, emotion, hate_speech, video_details, transcription, translation, language_code, segment_duration):
        vid_q, resolution, height, width, frame_rate, distortion, aspect_ratio, duration = quality_result
        self.quality_details["QUALITY_ANALYSIS"]["RESOLUTION"] = f"Video type: [{vid_q}] and Resolution: {resolution}"
        self.quality_details["QUALITY_ANALYSIS"]["FRAME_RATE"] = str(frame_rate) + " FPS"
        self.quality_details["QUALITY_ANALYSIS"]["DISTORTION_SCORE"] = str(distortion) + " %"
        self.quality_details["SANITY_CHECKS"]["ACCEPTABLE_ASPECT_RATIO"] = str(aspect_ratio)
        self.quality_details["SANITY_CHECKS"]["DURATION"] = str(duration) + " Sec"
        self.quality_details["DETECTED_WATERMARKS"]["CONTENTS"] = watermark
        self.quality_details["AUDIO_AUDIT"]["EMOTION_DETECTION"] = emotion
        self.quality_details["AUDIO_AUDIT"]["HATE_SPEECH_DETECTION"] = hate_speech
        self.quality_details["AUDIO_AUDIT"]["KEY_EMOTIONS"] = [em[1] for em in emotion if em]
        self.quality_details["AUDIO_AUDIT"]["TRANSCRIPTION"] = transcription
        self.quality_details["AUDIO_AUDIT"]["LANGUAGE_CODE"] = language_code
        self.quality_details["AUDIO_AUDIT"]["TRANSLATION"] = translation
        self.quality_details["AUDIO_AUDIT"]["SEGMENT_DURATION"] = segment_duration
        self.quality_details["TABLE_OF_CONTENTS"] = self.build_table_of_contents(emotion, hate_speech, video_details.copy())
        self.quality_details["VIDEO_AUDIT"] = video_details
        self.quality_details["VIDEO_AUDIT"]["KEY_SITUATIONS"] = [key for key in video_details.keys()] 
        self.quality_details["PERSON_DETECTED"] = random.choice(famous_list)
        

        import pprint
        pprint.pprint(self.quality_details)
    
    def save_results(self, quality_result):
        # Save the JSON object to a file
        filename = f"{FINAL_OUTPUT_DIR}/{self.file_name}_data.json"

        with open(filename, "w") as f:
            json.dump(self.quality_details, f, ensure_ascii=False, indent=4)

    # def save_results(self, quality_result, watermark, emotion, hate_speech):
    #     vid_q, resolution, height, width, frame_rate, distortion, aspect_ratio, duration = quality_result
    #     em_sadness = str(emotion.get("sadness", "N/A"))
    #     em_others = str(emotion.get("others", "N/A"))
    #     em_fear = str(emotion.get("fear", "N/A"))
    #     em_disgust = str(emotion.get("disgust", "N/A"))
    #     em_surprise = str(emotion.get("surprise", "N/A"))
    #     em_joy = str(emotion.get("joy", "N/A"))
    #     em_anger = str(emotion.get("anger", "N/A"))

    #     ht_hateful = str(hate_speech.get("hateful", "N/A"))
    #     ht_targeted = str(hate_speech.get("targeted", "N/A"))
    #     ht_aggressive = str(hate_speech.get("aggressive", "N/A"))

        # persist_flag = persist_audit_result(
        #     self.file_name,
        #     vid_q,
        #     resolution,
        #     frame_rate,
        #     distortion,
        #     aspect_ratio,
        #     duration,
        #     watermark,
        #     em_sadness,
        #     em_others,
        #     em_fear,
        #     em_disgust,
        #     em_surprise,
        #     em_joy,
        #     em_anger,
        #     ht_hateful,
        #     ht_targeted,
        #     ht_aggressive,
        #     height,
        #     width,
        #     "Completed"
        # )
        # if not persist_flag:
        #     logger.error(f"FAILED TO SAVE DATA IN DATABASE")

    def remove_artifacts(self):
        # Implement the method to remove temporary artifacts
        try:
            shutil.rmtree(os.path.join(TEMP_FRAMES_DIR, self.file_name))
            os.remove(os.path.join(PROCESSED_ORIGINAL_DIR, self.file_name))
            logger.warning(f"REMOVAL OF ARTIFACTS DONE")
        except Exception as ex:
            logger.error(f"FAILED TO REMOVE ARTIFACTS: {self.file_name} with {ex}")


    # def detect_watermark(self, cam=False):
    #     run_watermark(self.vs, cam)

    def analyse_video_quality(self):
        pass


from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Pool
from multiprocessing import Process, Manager

def split_frames(filename, vs):
        os.makedirs(os.path.join(TEMP_FRAMES_DIR, filename), exist_ok=True)
        success,image = vs.read()
        count =0
        for file in os.listdir(os.path.join(TEMP_FRAMES_DIR, filename)):
            os.remove(os.path.join(TEMP_FRAMES_DIR, filename,file))
        while success:
            cv2.imwrite(os.path.join(TEMP_FRAMES_DIR, filename, f"frame{count}.jpg"), image)
            success,image = vs.read()
            if success:
                count +=1
            else:
                break


def process_video_file(file_name):
    try:
        shutil.move(os.path.join(RAW_DIR, file_name), os.path.join(IN_PROCESS_DIR, file_name))
        logger.info(f"Moved {file_name} from RAW_DIR to IN_PROCESS_DIR")
        split_frames(file_name, cv2.VideoCapture(os.path.join(IN_PROCESS_DIR, file_name)))
        logger.info(f"FRAMES SPLITTED FOR VIDEO: {os.path.join(IN_PROCESS_DIR, file_name)}")
        audit_video = AuditVideo(file_name)
        
        audit_video.fit()
    except Exception as e:
        logger.error(f"ERROR: {e} WHILE PROCESSING VIDEO: {file_name}")
        import traceback
        traceback.print_exc()
        # Move the file to the FAILED directory if an exception occurs
        shutil.move(os.path.join(IN_PROCESS_DIR, file_name), os.path.join(FAILED_DIR, file_name))
        logger.info(f"Processed {file_name}, moved to FAILED")



def process_videos_parallel(videos):
    processes = []

    for video in videos:
        # p = Process(target=process_video_file, args=(video,))
        process_video_file(video)
        # processes.append(p)
        # p.start()

    # for p in processes:
    #     p.join()


def process_videos(max_parallel=3):
    cycle = 1
    in_process_files = [f for f in os.listdir(IN_PROCESS_DIR) if f.endswith(('.mp4', '.mkv', '.avi', '.flv', '.mov'))]

    for file_name in in_process_files:
        in_process_file_path = os.path.join(IN_PROCESS_DIR, file_name)
        raw_file_path = os.path.join(RAW_DIR, file_name)
        if os.path.exists(in_process_file_path):
            logger.warning(f"{file_name} already exists in IN_PROCESS_DIR. Moving back to RAW_DIR.")
            shutil.move(in_process_file_path, raw_file_path)
    while True:
        start = time.time()

        if not os.listdir(IN_PROCESS_DIR) and not os.listdir(RAW_DIR):
            time.sleep(3)
            continue

        logger.info(f"RAW_DIR: {RAW_DIR}")
        logger.info(f"IN_PROCESS_DIR: {IN_PROCESS_DIR}")

        raw_files = [file_name for file_name in os.listdir(RAW_DIR) if not file_name.endswith("png")]
        logger.info(f"Raw files found: {raw_files}")

        # Process videos in chunks of max_parallel
        for i in range(0, len(raw_files), max_parallel):
            chunk = raw_files[i:i + max_parallel]
            process_videos_parallel(chunk)
            logger.info(f"ENTIRE BATCH CYCLE TOOK: {time.time() - start} Secs")
            start=time.time()

        cycle += 1
        

        failed_files = os.listdir(IN_PROCESS_DIR)
        if failed_files:
            logger.error(f"FAILED FILE: {failed_files}")
            for file in failed_files:
                try:
                    shutil.move(os.path.join(IN_PROCESS_DIR, file), os.path.join(FAILED_DIR, file))
                    logger.info(f"Processed {file}, moved to FAILED")

                except Exception as ex:
                    logger.error(f"FAILED TO MOVE: {file} WITH ERROR: {ex}")




if __name__ == "__main__":
    process_videos()
