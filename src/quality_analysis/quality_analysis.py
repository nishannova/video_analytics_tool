from copy import deepcopy
from functools import lru_cache
import os
import gc
import random
import shutil
import cv2
# from imquality import brisque
import imquality.brisque as brisque
import sys
sys.path.append("/home/saintadmin/work/video_analytics_tool/src")

import PIL.Image
import imageio
from loguru import logger
import ray
import warnings

from skimage import io, img_as_float

from config import TEMP_FRAMES_DIR

warnings.filterwarnings("ignore")

class QualityAnalysis():
    def __init__(self, vs, filename):
        self.vs = vs
        self.filename = filename
        # self.split_frames(self.vs) 
    def split_frames(self):
        os.makedirs(os.path.join(TEMP_FRAMES_DIR, self.filename), exist_ok=True)
        success,image = self.vs.read()
        count =0
        for file in os.listdir(os.path.join(TEMP_FRAMES_DIR, self.filename)):
            os.remove(os.path.join(TEMP_FRAMES_DIR, self.filename,file))
        while success:
            cv2.imwrite(os.path.join(TEMP_FRAMES_DIR, self.filename, f"frame{count}.jpg"), image)
            success,image = self.vs.read()
            if success:
                count +=1
            else:
                break

    
    def distortion_analysys(self):
        scores=[]
        images = os.listdir(os.path.join(TEMP_FRAMES_DIR, self.filename))
        sample_num = min(int(0.05*len(images)), 10)
        sample_image = random.sample(images, sample_num)
        
        logger.info(f"There are {len(images)} frames to be analyzed")
        # logger.debug(f"sample_image: {sample_image}")
        batch = 0
        # for batch_images in TODO
        # for i in range(0, len(sample_image), 20):
        distortion_objects = [get_score.remote(img, self.filename) for img in sample_image]
        scores.extend(ray.get(distortion_objects))
        ray.internal.free(distortion_objects)
        try:
            return sum(scores)/len(scores)
        except Exception as ex:
            logger.error(f"Error in Quality: {ex}")
            return 0


    def resolution_analysis(self):
        success,image = self.vs.read()
        # logger.debug(f"ANALYSING RESOLUTION, STATUS: {success}")
        if success:
            vid_qual = self._get_resolution(*image.shape[:2])
            vid_res = f"{image.shape[0]} x {image.shape[1]} Pixels"
            # return f"VIDEO QUALITY: [{vid_qual}] VIDEO RESOLUTION: {vid_res}"
            return (vid_qual, vid_res, image.shape[0], image.shape[1])
        else:
            "N/A", "N/A", 0, 0

    @lru_cache()
    def _get_resolution(self, x, y):
        # logger.debug(f"ANALYSING FRAME WITH Height: {y} Width: {x}")
        res_dict = {
            "SD": 640*480,
            "HD": 1280 * 720,
            "FHD": 1920 * 1080,
            "QHD": 2560 * 1440,
            "UHD": 3840 * 2160,
            "8K": 7680 * 4320,
            }
        img_area = x * y
        min_diff = float("inf")
        min_key = None
        for key, val in res_dict.items():
            if min_diff > abs(img_area - val):
                min_diff = abs(img_area - val)
                min_key = key
        # logger.debug(f"Identified Resolution: {min_key}")
        return min_key

    def frame_rate_analysis(self):
        fps = self.vs.get(cv2.CAP_PROP_FPS)
        return fps

    def duration(self):
        fps = self.vs.get(cv2.CAP_PROP_FPS)
        frame_count = self.vs.get(cv2.CAP_PROP_FRAME_COUNT)
        # logger.debug(f"Duration {frame_count/fps} seconds")
        try:
            return int(frame_count/fps)
        except Exception as ex:
            print("ERROR: {ex} in duration")
            return 0.0

    def aspect_ratio_analysis(self):
        success,image = self.vs.read()
        count =0
        while success:
            cv2.imwrite(os.path.join(TEMP_FRAMES_DIR, self.filename, f"frame{count}.jpg"), image)
            success,image = self.vs.read()
            if success:
                count +=1
            else:
                break
            break
        images = os.listdir(os.path.join(TEMP_FRAMES_DIR, self.filename))

        print(f"There are {len(images)} frames to be analyzed")
        count = 0
        for img in images:
            im = cv2.imread(os.path.join(TEMP_FRAMES_DIR, self.filename, img), cv2.COLOR_RGB2BGR).copy()
            H, W = im.shape[:2]
            break
        if W > H:
            return False
        else:
            return True


@ray.remote(scheduling_strategy="SPREAD")
def get_score(img, filename):
    import imquality.brisque as brisque
    score = 0
    im = img_as_float(io.imread(os.path.join(TEMP_FRAMES_DIR, filename, img), as_gray=False))
    try:
        score = brisque.score(im)
    except Exception as ex:
        print(f"Error while brisque: {ex}")
    return score

    

if __name__ == "__main__":
    path = "/Users/sali115/Video_Analytics_Tool_copy/data/raw/C045-03[1].mp4"
    vs = cv2.VideoCapture(path)
    quality_obj = QualityAnalysis(vs)
    print(f"VIDEO DISTORTION SCORE:{quality_obj.distortion_analysys()}")
