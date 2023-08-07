import cv2
import os
import numpy as np

def process_frame(frame, model, label, colors):
    results = model(frame)
    detected_objects = []

    if results[0].boxes.cls.tolist():
        objects = results[0].boxes.cls.tolist()
        co_ordinates = results[0].boxes.xyxyn.tolist()
        h, w, _ = frame.shape

        for i, obj in enumerate(objects):
            obj = int(obj)
            x1, y1, x2, y2 = co_ordinates[i]
            xmin, ymin, xmax, ymax = denormalize_coordinates(x1, y1, x2, y2, h, w)
            frame = draw_object(frame, obj, label, colors, xmin, ymin, xmax, ymax)
            detected_objects.append({"object": label[obj], "coordinates": (xmin, ymin, xmax, ymax)})

    return frame, detected_objects

def denormalize_coordinates(x1, y1, x2, y2, h, w):
    xmin = int(x1 * w)
    ymin = int(y1 * h)
    xmax = int(x2 * w)
    ymax = int(y2 * h)
    return xmin, ymin, xmax, ymax

def draw_object(frame, obj, label, colors, xmin, ymin, xmax, ymax):
    cv2.rectangle(frame, (xmin, ymin), (xmax, ymax), color=colors[obj], thickness=2)
    font_scale = min(1, max(3, int(frame.shape[1] / 500)))
    font_thickness = min(2, max(10, int(frame.shape[1] / 50)))
    p1, p2 = (int(xmin), int(ymin)), (int(xmax), int(ymax))
    tw, th = cv2.getTextSize(label[obj], 0, fontScale=font_scale, thickness=font_thickness)[0]
    p2 = p1[0] + tw, p1[1] + -th - 10
    cv2.rectangle(frame, p1, p2, color=colors[obj], thickness=-1,)
    cv2.putText(frame, label[obj], (xmin + 1, ymin - 10), cv2.FONT_HERSHEY_SIMPLEX, font_scale, (255, 255, 255), font_thickness)
    return frame

def video_audit(video_path, save_path, model):
    pred_frame = {}
    cap = cv2.VideoCapture(video_path)
    video_name = video_path.split('/')[-1].split('.')[0]
    os.makedirs(os.path.join(save_path, video_name), exist_ok=True)
    path_to_save_frame = os.path.join(save_path, video_name)
    label = { 0:'beer',1:'bombs',2:'cocaine',3:'other_alcohol',4:'other_drugs',5:'other_weapon',6:'pistols',7:'rifle',8:'shotgun', 9:'syringe', 10:'tobacco',11:'wine'}
    # label = {0: "Beer", 1: "Wine", 2: "Shotgun", 3: "Syringe", 4: "Knife", 5: "Bomb",6:"aasdas",7:'qwewq',8:'asfq',9:'3241',10:"3afaa",11:'adas'}
    colors = np.random.uniform(0, 255, size=(12, 3))


    # Process every 120th frame
    for frame_number in range(0, int(cap.get(cv2.CAP_PROP_FRAME_COUNT)), 120):
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
        ret, frame = cap.read()
        if not ret:
            break

        frame, detected_objects = process_frame(frame, model, label, colors)

        if detected_objects:
            timestamp = frame_number / cap.get(cv2.CAP_PROP_FPS)
            for obj_data in detected_objects:
                obj_label = obj_data["object"]
                if obj_label not in pred_frame:
                    pred_frame[obj_label] = {"Timestamp": set()}
                pred_frame[obj_label]["Timestamp"].add(timestamp)

                os.makedirs(os.path.join(path_to_save_frame, obj_label), exist_ok=True)
                cv2.imwrite(os.path.join(path_to_save_frame, obj_label, f'frame_{str(frame_number).zfill(4)}.png'), frame)

    # Convert sets back to lists for JSON compatibility
    for obj_label in pred_frame:
        pred_frame[obj_label]["Timestamp"] = list(pred_frame[obj_label]["Timestamp"])

    cap.release()
    
    cv2.destroyAllWindows()
    

    return pred_frame
