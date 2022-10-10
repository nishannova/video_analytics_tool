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
sys.path.append("/Users/sali115/Video_Analytics_Tool_copy/src")
sys.path.append("..")
import PIL.Image
import imageio
from loguru import logger

from skimage import io, img_as_float

from config import TEMP_FRAMES_DIR


class QualityAnalysis():
    def __init__(self, vs):
        self.vs = vs
    
    def split_frames(self, vs):
        success,image = vs.read()
        count =0
        for file in os.listdir(TEMP_FRAMES_DIR):
            os.remove(os.path.join(TEMP_FRAMES_DIR,file))
        while success:
            cv2.imwrite(os.path.join(TEMP_FRAMES_DIR, f"frame{count}.jpg"), image)
            success,image = vs.read()
            if success:
                count +=1
            else:
                break

    
    def distortion_analysys(self):

        scores=[]
        images = os.listdir(TEMP_FRAMES_DIR)
        sample_image = random.sample(images,5)
        
        logger.info(f"There are {len(images)} frames to be analyzed")
        count = 0
        for img in sample_image:
            im = img_as_float(io.imread(os.path.join(TEMP_FRAMES_DIR, img), as_gray=False))
            scores.append(brisque.score(im))
            if (count % 25) == 0:
                print(f"Analysis done for {count} th frame")
                gc.collect()
            count += 1
        return sum(scores)/len(scores)


    def resolution_analysis(self, vs):
        success,image = vs.read()
        logger.debug(f"ANALYSING RESOLUTION, STATUS: {success}")
        if success:
            vid_qual = self._get_resolution(*image.shape[:2])
            vid_res = f"{image.shape[0]} x {image.shape[1]} Pixels"
            return f"VIDEO QUALITY: [{vid_qual}] VIDEO RESOLUTION: {vid_res}"

    @lru_cache()
    def _get_resolution(self, x, y):
        logger.debug(f"ANALYSING FRAME WITH Height: {y} Width: {x}")
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
        logger.debug(f"Identified Resolution: {min_key}")
        return min_key

    def frame_rate_analysis(self, vs):
        fps = vs.get(cv2.CAP_PROP_FPS)
        return fps

    def duration(self, vs):
        fps = vs.get(cv2.CAP_PROP_FPS)
        frame_count = vs.get(cv2.CAP_PROP_FRAME_COUNT)
        logger.debug(f"Duration {frame_count/fps} seconds")
        return int(frame_count/fps)

    def aspect_ratio_analysis(self, vs):
        success,image = vs.read()
        count =0
        while success:
            cv2.imwrite(os.path.join(TEMP_FRAMES_DIR, f"frame{count}.jpg"), image)
            success,image = self.vs.read()
            if success:
                count +=1
            else:
                break
            break
        images = os.listdir(TEMP_FRAMES_DIR)

        print(f"There are {len(images)} frames to be analyzed")
        count = 0
        for img in images:
            im = cv2.imread(os.path.join(TEMP_FRAMES_DIR, img), cv2.COLOR_RGB2BGR).copy()
            H, W = im.shape[:2]
            break
        if W > H:
            return False
        else:
            return True

if __name__ == "__main__":
    path = "/Users/sali115/Video_Analytics_Tool_copy/data/raw/C045-03[1].mp4"
    vs = cv2.VideoCapture(path)
    quality_obj = QualityAnalysis(vs)
    print(f"VIDEO DISTORTION SCORE:{quality_obj.distortion_analysys()}")
