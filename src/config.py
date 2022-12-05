import os

DATA_ROOT = "/home/saintadmin/work/video_analytics_tool/data"
TEMP_FRAMES_DIR = os.path.join(DATA_ROOT, "processed/temp_frames")
EAST_MODEL_PATH = os.path.join(DATA_ROOT, "model/frozen_east_text_detection.pb")
WATERMARK_PATH = os.path.join(DATA_ROOT, "processed/watermark_detection")
VIDEO_PATH = os.path.join(DATA_ROOT, "processed/recorded_video")

RAW_DIR = os.path.join(DATA_ROOT, "raw")
PROCESSED_ORIGINAL_DIR = os.path.join(DATA_ROOT, "processed", "original_video")
IN_PROCESS_DIR = os.path.join(DATA_ROOT, "in_process")
FAILED_DIR = os.path.join(DATA_ROOT, "failed_files")