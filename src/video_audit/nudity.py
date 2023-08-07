import cv2
import numpy as np
import os
import tensorflow as tf

# def video_audit_nudity(video_path, save_path, model):
#     # import keras
#     pred_frame = {}
#     cap = cv2.VideoCapture(video_path)
#     video_name = video_path.split('/')[-1].split('.')[0]
#     os.makedirs(os.path.join(save_path, video_name), exist_ok=True)
#     path_to_save_frame = os.path.join(save_path, video_name)
#     threshold = 0.8

#     # Process every 12th frame
#     for frame_number in range(0, int(cap.get(cv2.CAP_PROP_FRAME_COUNT)), 120):
#         cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
#         ret, frame = cap.read()
#         if not ret:
#             break

#         # Nudity detection
#         frame_resized = cv2.resize(frame, (224, 224))
#         frame_resized = np.expand_dims(frame_resized, axis=0)
#         frame_resized = frame_resized / 255.0

#         score = model.predict(frame_resized,verbose = 0)[0][0]

#         if score >= threshold:
#             timestamp = frame_number / cap.get(cv2.CAP_PROP_FPS)

#             if 'nudity' not in pred_frame:
#                 pred_frame['nudity'] = {"Timestamp": set()}
#             pred_frame['nudity']["Timestamp"].add(timestamp)

#             os.makedirs(os.path.join(path_to_save_frame, 'nudity'), exist_ok=True)
#             cv2.imwrite(os.path.join(path_to_save_frame, 'nudity', f'frame_{str(frame_number).zfill(4)}.png'), frame)

#     # Convert sets back to lists for JSON compatibility
#     if 'nudity' not in pred_frame:
#         return False
#     pred_frame['nudity']["Timestamp"] = list(pred_frame['nudity']["Timestamp"])

#     cap.release()
#     # cv2.destroyAllWindows()

#     return pred_frame
def video_audit_nudity(video_path, save_path, model):
    pred_frame = {}
    cap = cv2.VideoCapture(video_path)
    video_name = video_path.split('/')[-1].split('.')[0]
    os.makedirs(os.path.join(save_path, video_name), exist_ok=True)
    path_to_save_frame = os.path.join(save_path, video_name)
    threshold = 0.87

    # Process every 12th frame
    for frame_number in range(0, int(cap.get(cv2.CAP_PROP_FRAME_COUNT)), 120):
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
        ret, frame = cap.read()
        if not ret:
            break

        # Nudity detection
        frame_resized = cv2.resize(frame, (224, 224))
        frame_resized = np.expand_dims(frame_resized, axis=0)
        frame_resized = frame_resized / 255.0

        score = model.predict(frame_resized, verbose=0)[0][0]

        if score >= threshold:
            timestamp = frame_number / cap.get(cv2.CAP_PROP_FPS)

            if 'nudity' not in pred_frame:
                pred_frame['nudity'] = {"Timestamp": []}
            
            pred_frame['nudity']["Timestamp"].append(timestamp)

            os.makedirs(os.path.join(path_to_save_frame, 'nudity'), exist_ok=True)
            cv2.imwrite(os.path.join(path_to_save_frame, 'nudity', f'frame_{str(frame_number).zfill(4)}.png'), frame)

    cap.release()

    # if 'nudity' not in pred_frame:
    #     return False

    return pred_frame

