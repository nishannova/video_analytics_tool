#app.py
from flask import Flask, flash, request, redirect, url_for, render_template, Response
import urllib.request
import os
from werkzeug.utils import secure_filename
from loguru import logger
import sys
sys.path.append('/Users/nishanali/WorkSpace/Video_Analytics_Tool/src')
from main import run_main
from quality_analysis.quality_analysis import QualityAnalysis
from watermark_detection.detect import process_detection
# from ui import demo_ui
import cv2
import shutil
import time

app = Flask(__name__, static_folder='static')
 
UPLOAD_FOLDER = 'static/uploads'
 
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
 
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg', 'gif', 'mp4'])
 
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
     
class VideoCamera(object):
    def __init__(self,file_name):
        self.file_name = file_name
        self.path = "/Users/nishanali/WorkSpace/Video_Analytics_Tool/data/processed/recorded_video"
        self.video = cv2.VideoCapture(os.path.join(self.path, self.file_name))
        

    def get_frame(self):
        success, image = self.video.read()
        if success:
            ret, jpeg = cv2.imencode('.jpg', image)
            return jpeg.tobytes()
        return None



def gen(camera):
    while True:
        frame = camera.get_frame()
        if frame:
            yield (b'--frame\r\n'
                b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n\r\n')

@app.route('/')
def home():
    return render_template('index.html')
 
@app.route('/', methods=['POST'])
def upload_image():
    quality_details = dict()
    quality_details = dict()
    quality_details["QUALITY_ANALYSIS"] = {}
    quality_details["DETECTED WATERMARKS"] = {}
    quality_details["SANITY CHECKS"] = {}
    path = "/Users/nishanali/WorkSpace/Video_Analytics_Tool/services/static/uploads"
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        flash('No image selected for uploading')
        return redirect(request.url)
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        # flash('VIDEO FILE UPLOADED...!!!')
        # return redirect(request.url)
    else:
        flash('Allowed image types are - png, jpg, jpeg, gif, mp4')
        return redirect(request.url)

    if not os.listdir("/Users/nishanali/WorkSpace/Video_Analytics_Tool/services/static/uploads") and cam != "YES":
        flash('NO VIDEO FILES TO DETECT')
        return redirect(request.url)

    for file_name in os.listdir(path):
        if file_name.endswith("png"):
            continue
        if not file_name in filename:
            logger.warning(f"SKIPPING: {file_name} matched with: {filename}")
            continue

        q_obj = QualityAnalysis(cv2.VideoCapture(os.path.join(path, file_name)))
        q_obj.split_frames(cv2.VideoCapture(os.path.join(path, file_name)))
        logger.warning("FRAME SPLITTING DONE...!!!")

        quality_details["QUALITY_ANALYSIS"]["RESOLUTION"] = q_obj.resolution_analysis(cv2.VideoCapture(os.path.join(path, file_name)))
        quality_details["QUALITY_ANALYSIS"]["FRAME RATE"] = str(int(q_obj.frame_rate_analysis(cv2.VideoCapture(os.path.join(path, file_name)))))+" FPS"
        quality_details["QUALITY_ANALYSIS"]["DISTORTION SCORE"] =  str(round(q_obj.distortion_analysys(), 2))+" %"
        quality_details["SANITY CHECKS"]["ACCEPTABLE ASPECT RATIO"] = q_obj.aspect_ratio_analysis(cv2.VideoCapture(os.path.join(path, file_name)))
        quality_details["SANITY CHECKS"]["DURATION"] = str(q_obj.duration(cv2.VideoCapture(os.path.join(path, file_name))))+" Secs"
        watermark = process_detection()
        quality_details["DETECTED WATERMARKS"]["CONTENTS"] =  list(watermark) if watermark else ["No Watermarks Found" ]
    logger.info(quality_details)
    
    return render_template('index.html', filename=str(filename.split(".")[0])+"_detected.avi", quality_details=quality_details)

    


@app.route('/display/<filename>')
def display_image(filename):
    logger.warning(f"Trying to display {filename}")
    return Response(gen(VideoCamera(filename)),
                mimetype='multipart/x-mixed-replace; boundary=frame')

 
if __name__ == "__main__":
    app.run(debug=True)