import tensorflow as tf
import numpy as np
from skimage.io import imread
from skimage.transform import resize
import cv2
import os

def pred_fight(model,video,acuracy=0.9):
    pred_test = model.predict(video, verbose=0)
    if pred_test[0][1] >=acuracy:
        return True , pred_test[0][1]
    else:
        return False , pred_test[0][1]

def video_audit_violence(video_path, save_path, model_violence):
    pred_frame = {}
    cap = cv2.VideoCapture(video_path)
    video_name = os.path.splitext(os.path.basename(video_path))[0]
    os.makedirs(os.path.join(save_path, video_name, 'violence'), exist_ok=True)
    threshold = 0.9

    for frame_number in range(0, int(cap.get(cv2.CAP_PROP_FRAME_COUNT)), 120):
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
        ret, frame = cap.read()
        if not ret:
            break

        frame_resized = cv2.resize(frame, (160, 160))
        frame_resized = np.expand_dims(frame_resized, axis=0)
        if np.max(frame_resized) > 1:
            frame_resized = frame_resized / 255.0

        frames = np.zeros((1, 30, 160, 160, 3), dtype=float)
        frames[0][:][:] = frame_resized

        prediction, percent = pred_fight(model_violence, frames, acuracy=0.85)

        if prediction:
            timestamp = frame_number / cap.get(cv2.CAP_PROP_FPS)

            if 'violence' not in pred_frame:
                pred_frame['violence'] = {"Timestamp": []}
            pred_frame['violence']["Timestamp"].append(timestamp)

            cv2.imwrite(os.path.join(save_path, video_name, f'frame_{str(frame_number).zfill(4)}.png'), frame)

    cap.release()

    return pred_frame


# video_path = "/content/drive/MyDrive/yolov8_large/John Wick_ Chapter 3 - Parabellum (2019) - Throwing Knives Scene (1_12) _ Movieclips.mp4"
# violence_timestamps = video_audit_violence(video_path, save_path, model22)
# print("Violence Timestamps (in seconds):", violence_timestamps)
