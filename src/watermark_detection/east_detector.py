from config import EAST_MODEL_PATH, WATERMARK_PATH
import cv2
from loguru import logger
import numpy as np
from imutils.object_detection import non_max_suppression
import pytesseract
import os
import ray

@ray.remote(scheduling_strategy="SPREAD")
def east_detector(img_path):
    file_name = img_path.split("/")[-1].split(".")[0]
    os.makedirs(os.path.join(WATERMARK_PATH,file_name), exist_ok=True)
    image1 = cv2.imread(img_path,cv2.IMREAD_COLOR) 
    ima_org = image1.copy()
    (height1, width1) = image1.shape[:2]
    size = 1280 
    (height2, width2) = (size, size)  
    image2 = cv2.resize(image1, (width2, height2))  

    blur = cv2.bilateralFilter(image2,5,55,60)

    image2 = blur.copy()
    net = cv2.dnn.readNet(EAST_MODEL_PATH)
    blob = cv2.dnn.blobFromImage(image2, 1.0, (width2, height2), (123.68, 116.78, 103.94), swapRB=True, crop=False)
    net.setInput(blob)

    (scores, geometry) = net.forward(
        ["feature_fusion/Conv_7/Sigmoid", 
        "feature_fusion/concat_3"]
        )

    (rows, cols) = scores.shape[2:4]  
    rects = []  
    confidences = []  

    for y in range(rows):
        scoresdata = scores[0, 0, y]
        xdata0 = geometry[0, 0, y]
        xdata1 = geometry[0, 1, y]
        xdata2 = geometry[0, 2, y]
        xdata3 = geometry[0, 3, y]
        angles = geometry[0, 4, y]

        for x in range(cols):

            if scoresdata[x] < 0.75:  
                continue
            
            offsetx = x * 4.0
            offsety = y * 4.0

            angle = angles[x]
            cos = np.cos(angle)
            sin = np.sin(angle)

            h = xdata0[x] + xdata2[x]
            w = xdata1[x] + xdata3[x]
            
            endx = int(offsetx + (cos * xdata1[x]) + (sin * xdata2[x]))
            endy = int(offsety + (sin * xdata1[x]) + (cos * xdata2[x]))
            startx = int(endx - w)
            starty = int(endy - h)

            
            rects.append((startx, starty, endx, endy))
            confidences.append(scoresdata[x])



    boxes = non_max_suppression(np.array(rects), probs=confidences)

    iti=[]
    rW = width1 / float(width2)
    rH = height1 / float(height2)


    bb = []
    for (startx, starty, endx, endy) in boxes:
        startx = int(startx * rW)
        starty = int(starty * rH)
        endx = int(endx * rW)
        endy = int(endy * rH)
        cv2.rectangle(image1, (startx, starty), (endx, endy), (255, 0,0), 2)
        
        bb.append([startx, starty, endx, endy])
        


    csx = 0
    cex = 0
    csy = 0
    cey = 0
    for i,box in enumerate(bb[::-1]):
        if i==0:
            csx = box[0]
        else:
            cex = box[2]
            cey = box[3]+5
            esx = box[1]
        
    x=[]
    y=[]
    w=[]
    h=[]
    for bbox in bb:
        x.append(bbox[0])
        y.append(bbox[1])
        w.append(bbox[2])
        h.append(bbox[3])
    if x and y and w and h:
        final_x,final_y,final_w,final_h=min(x),min(y),max(w),max(h)


        it=ima_org[final_y:final_h,final_x:final_w]
        if it.any():
            # blur = cv2.bilateralFilter(it,5,55,60)
            cv2.imwrite(os.path.join(WATERMARK_PATH,file_name, str(file_name)+"_smooth.jpg"), it)
            gray = cv2.cvtColor(blur,cv2.COLOR_BGR2GRAY)
            cv2.imwrite(os.path.join(WATERMARK_PATH,file_name,str(file_name)+"_gray.jpg"), gray)
            thresh = cv2.threshold(gray,220,255,1,cv2.THRESH_OTSU + cv2.THRESH_BINARY_INV)[1]
            cv2.imwrite(os.path.join(WATERMARK_PATH,file_name,str(file_name)+"_binarized.jpg"), thresh)
            txt = pytesseract.image_to_string(it,lang='eng',config='--oem 3 --psm 11')

            return txt.strip()
    else:
        return ""