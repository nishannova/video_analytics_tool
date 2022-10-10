from watermark_detection.watermark import run_watermark
import cv2

class Main():
    def __init__(self, path):
        self.vs = cv2.VideoCapture(path)
    
    def fit(self):
        # quality = analyse_video(self.video)
        # water_mark = analyse_watermark(self.video)
        # similarity_score = analyse_similarity(self.video_1, self.video_2)
        # profanity = analyse_profanity(self.video)
        pass
    
    def detect_watermark(self, cam=False):
        run_watermark(self.vs, cam)

    def analyse_video_quality(self):
        pass

# if __name__ == "__main__":
def run_main(path, cam):
#     path = "/Users/sali115/Video_Analytics_Tool_copy/data/raw/C033-03[1].mp4"
#     cam=False
    video_analytics = Main(path)
    video_analytics.detect_watermark(cam)
