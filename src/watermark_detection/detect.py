from functools import lru_cache
import sys
# sys.path.append("/Users/sali115/Video_Analytics_Tool_copy/src")
from config import TEMP_FRAMES_DIR

import os
import random
from loguru import logger
from watermark_detection.east_detector import east_detector
from functools import lru_cache
import ray
import re

def process_detection(filename):
    images = os.listdir(os.path.join(TEMP_FRAMES_DIR,filename))
    texts = list()
    filtered = list()

    sample_image = list()
    for i in range(0, len(images), 60):
        sample_image.append(images[i])
    logger.warning(f"SAMPLE IMAGE: {sample_image}")
    for i in range(0, len(sample_image), 10):
        print("TEMP_FRAMES_DIR:", TEMP_FRAMES_DIR)
        print(f"FILES: {os.listdir(TEMP_FRAMES_DIR)}")
        text_list = ray.get([east_detector.remote(os.path.join(TEMP_FRAMES_DIR,filename, frame)) for frame in sample_image[i:i+100] if frame])

    
    # for frame in sample_image:
    #     texts.append(east_detector(os.path.join(TEMP_FRAMES_DIR, filename, frame)))
    # if texts:
    #     text_list=texts.copy()
    try:
        if text_list:
            for text in text_list:
                if text:
                    _ = [texts.append(x) for x in text.split("\n")]
            for id, txt in enumerate(texts):
                # logger.debug(f"PROCESSING: {txt} len(x): {len(txt)} Index: {id}")
                if (not txt) or\
                    (len(txt.strip()) < 6) or\
                        "(" in txt.strip() or\
                            ")" in txt.strip()\
                                or "@" in txt.strip()\
                                    or "\\" in txt.strip()\
                                        or "<" in txt.strip()\
                                            or "~" in txt.strip()\
                                                or "«" in txt.strip()\
                                                    or "©" in txt.strip():
                    # if not "mx" in txt.lower() or "tak" not in txt.lower():
                    continue
                else:
                    filtered.append(txt.strip())
            if filtered:
                filtered = list(set(filtered))
                logger.warning(f"REMOVAL OF DUPLICATES DONE ON WATERMARK")
                filtered = " ".join(filtered)
                filtered = re.sub('[^a-zA-Z0-9\.%:/]', ' ', filtered)
                logger.warning(f"REMOVAL OF UNWANTED NOISE DONE ON WATERMARK")
    except Exception as ex:
        logger.error(f"Failed to process watermark: {ex}")
        return "No Watermark Found"
    logger.warning(f"DETECTED TEXTS: {filtered}")

    return filtered

if __name__ == "__main__":
    process_detection()