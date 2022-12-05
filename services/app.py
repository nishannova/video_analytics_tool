#app.py
from flask import Flask, flash, request, redirect, url_for, render_template, Response
import urllib.request
import os
from requests import session
from werkzeug.utils import secure_filename
from loguru import logger
import sys
sys.path.append(".")
sys.path.append("..")

from src.quality_analysis.quality_analysis import QualityAnalysis
from src.watermark_detection.detect import process_detection
from src.config import TEMP_FRAMES_DIR, VIDEO_PATH
import cv2
import shutil
import time
from db.db import fetch_records
import json

from flask_sqlalchemy import SQLAlchemy


app = Flask(__name__, static_folder='static')
 
UPLOAD_FOLDER = '/home/saintadmin/work/video_analytics_tool/data/raw'

app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
# app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:///project.db"


# db = SQLAlchemy(app) 


# class VideoProperties(db.Model):
#     id = db.Column(db.Integer, primary_key=True)
#     name = db.Column(db.String, nullable=False)
#     resolution = db.Column(db.String)
#     quality = db.Column(db.String)
#     frame_rate = db.Column(db.Integer)
#     distortion_score = db.Column(db.Float)
#     watermark = db.Column(db.String)
#     aspect_ratio = db.Column(db.Boolean)
#     duration = db.Column(db.Float)



# with app.app_context():
#     db.create_all()



ALLOWED_EXTENSIONS = set(['mp4', 'avi'])
 
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

def remove_artifacts(filename):
    shutil.rmtree(os.path.join(TEMP_FRAMES_DIR,filename))
    os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename))

@app.route('/')
def home():
    return render_template('index.html')
 
@app.route('/', methods=['POST'])
def upload_image():
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
        flash('VIDEO FILE UPLOADED...!!!')
        return redirect(request.url)
    else:
        flash('Allowed image types are - mp4, avi')
        return redirect(request.url)

    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_video():
    if 'file' not in request.files:
        # flash('No file part')
        return {"Message": "No files present"}
    file = request.files['file']
    if file.filename == '':
        # flash('No image selected for uploading')
        return {"Message": "No files present"}
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        # flash('VIDEO FILE UPLOADED...!!!')
        # return redirect(request.url)
    else:
        # flash('Allowed image types are - mp4, avi')
        return {"Message": "Allowed image types are - mp4, avi"}

    return {"Message": f"File Uploaded: {file.filename}"}
    


@app.route('/display/<filename>')
def display_image(filename):
    logger.warning(f"Trying to display {filename}")
    return Response(gen(VideoCamera(filename)),
                mimetype='multipart/x-mixed-replace; boundary=frame')

  
@app.route("/extract_record")
def extract_data():
    try:
        # filename = secure_filename(request.args.get("filename", ""))
        filename = request.args.get("filename", "")
        response = fetch_records(filename)
        # record = VideoProperties.query.filter_by(name = filename).first()

        # response = jsonify({"name": record.name, "resolution": record.resolution, 
        #             "frame_rate": record.frame_rate, "distortion_score": record.distortion_score,
        #             "watermark": record.watermark, "aspect_ratio": record.aspect_ratio,
        #             "duration": record.duration})
        # response.status_code = 201
    except Exception as ex:
        response = json.dumps({"output": "Failure", "error": str(ex)})
        # response.status_code = 500

    return response

 
if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
