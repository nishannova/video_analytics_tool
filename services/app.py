#app.py
from flask import Flask, flash, request, redirect, url_for, render_template, Response
import urllib.request
import os
from requests import session
from werkzeug.utils import secure_filename
from loguru import logger
import sys
sys.path.append('/Users/nishanali/WorkSpace/video_analytics_tool_distributed/src')
from main import run_main
from quality_analysis.quality_analysis import QualityAnalysis
from watermark_detection.detect import process_detection
from config import TEMP_FRAMES_DIR, VIDEO_PATH
import cv2
import shutil
import time


from flask_sqlalchemy import SQLAlchemy


app = Flask(__name__, static_folder='static')
 
UPLOAD_FOLDER = 'static/uploads'

app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///project.db"


db = SQLAlchemy(app) 


class VideoProperties(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    resolution = db.Column(db.String)
    quality = db.Column(db.String)
    frame_rate = db.Column(db.Integer)
    distortion_score = db.Column(db.Float)
    watermark = db.Column(db.String)
    aspect_ratio = db.Column(db.Boolean)
    duration = db.Column(db.Float)



with app.app_context():
    db.create_all()



ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg', 'gif', 'mp4'])
 
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
     
class VideoCamera(object):
    def __init__(self,file_name):
        self.file_name = file_name
        self.path = VIDEO_PATH
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

def persist_audit_result(filename, vid_q, resolution, frame_rate, distortion, aspect_ratio, duration, watermark):
    name = filename
    resolution = resolution
    quality = vid_q
    frame_rate = int(frame_rate)
    distortion_score = distortion
    watermark = watermark
    aspect_ratio = aspect_ratio
    duration = duration
    vid_res = VideoProperties(
        name=name, 
        resolution=resolution,
        quality=quality,
        frame_rate=frame_rate,
        distortion_score=distortion_score,
        watermark=watermark,
        aspect_ratio=aspect_ratio,
        duration=duration
        )
    db.session.add(vid_res)
    db.session.commit()
    return True

def remove_artifacts(filename):
    shutil.rmtree(os.path.join(TEMP_FRAMES_DIR,filename))
    os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename))

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
    path = app.config['UPLOAD_FOLDER']
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

    if not os.listdir(app.config['UPLOAD_FOLDER']):
        flash('NO VIDEO FILES TO DETECT')
        return redirect(request.url)

    for file_name in os.listdir(path):
        if file_name.endswith("png"):
            continue
        if not file_name in filename:
            logger.warning(f"SKIPPING: {file_name} matched with: {filename}")
            continue

        q_obj = QualityAnalysis(cv2.VideoCapture(os.path.join(path, filename)), filename)
        q_obj.split_frames(cv2.VideoCapture(os.path.join(path, file_name)))
        logger.warning("FRAME SPLITTING DONE...!!!")
        
        vid_q, resolution, frame_rate, distortion, aspect_ratio, duration, watermark = None, None, None, None, None, None, None
        
        vid_q, resolution = q_obj.resolution_analysis(cv2.VideoCapture(os.path.join(path, file_name)))
        frame_rate = int(q_obj.frame_rate_analysis(cv2.VideoCapture(os.path.join(path, file_name))))
        distortion = round(q_obj.distortion_analysys(), 2)
        aspect_ratio = q_obj.aspect_ratio_analysis(cv2.VideoCapture(os.path.join(path, file_name)))
        duration = q_obj.duration(cv2.VideoCapture(os.path.join(path, file_name)))
        watermark = process_detection(filename)
        watermark = str(watermark) if watermark else "No Watermarks Found"
    
        
        quality_details["QUALITY_ANALYSIS"]["RESOLUTION"] = f"Video type: [{vid_q}] and Resolution: {resolution}"
        quality_details["QUALITY_ANALYSIS"]["FRAME RATE"] = str(frame_rate) + " FPS"
        quality_details["QUALITY_ANALYSIS"]["DISTORTION SCORE"] = str(distortion) + " %"
        quality_details["SANITY CHECKS"]["ACCEPTABLE ASPECT RATIO"] = str(aspect_ratio)
        quality_details["SANITY CHECKS"]["DURATION"] = str(duration) + " Sec"
        quality_details["DETECTED WATERMARKS"]["CONTENTS"] = watermark.split(",")
        

    logger.info(quality_details)
    persist_flag = persist_audit_result(filename, vid_q, resolution, frame_rate, distortion, aspect_ratio, duration, watermark)
    if persist_flag:
        logger.info("Successfullt Persisted Records")
    remove_artifacts(filename)
    logger.info("Successfully removed all the temporary artifacts")
    return render_template('index.html', filename=str(filename.split(".")[0])+"_detected.avi", quality_details=quality_details)

    


@app.route('/display/<filename>')
def display_image(filename):
    logger.warning(f"Trying to display {filename}")
    return Response(gen(VideoCamera(filename)),
                mimetype='multipart/x-mixed-replace; boundary=frame')

 
# if __name__ == "__main__":
#     app.run()