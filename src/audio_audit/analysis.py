from __future__ import print_function
import os
import boto3
import time
import urllib
import json
from deep_translator import GoogleTranslator
from pysentimiento import create_analyzer
from loguru import logger
import time
from .upload import upload_video_for_transcribe

transcribe_client = boto3.client('transcribe')
emotion_analyzer = create_analyzer(task="emotion", lang="en")                          
hate_speech_analyzer = create_analyzer(task="hate_speech", lang="en")


def transcribe_file(job_name, file_uri, transcribe_client):   

    transcribe_client.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': file_uri},
        MediaFormat=file_uri.split(".")[-1],
        # LanguageOptions=('hi-IN','en-IN', 'mr-IN','te-IN'),
        IdentifyLanguage = True
    )

    max_tries = 60
    while max_tries > 0:
        max_tries -= 1
        job = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
        job_status = job['TranscriptionJob']['TranscriptionJobStatus']
        if job_status in ['COMPLETED', 'FAILED']:
            print(f"Job {job_name} is {job_status}.")
            if job_status == 'COMPLETED':
                response = urllib.request.urlopen(job['TranscriptionJob']['Transcript']['TranscriptFileUri'])
                data = json.loads(response.read())
                text = data['results']['transcripts'][0]['transcript']
                print("========== below is output of speech-to-text ========================")
                print(text)
                print("=====================================================================")
                return text
            break
        else:
            print(f"Waiting for {job_name}. Current status is {job_status}.")
        time.sleep(10)

def process_audio(file_path):
    try:
        if upload_video_for_transcribe(file_path):
            logger.info(f"FILE UPLOADED SUCCESSFULLY, PROCESSING WITH TRANSCRIPTION")
        file_uri = f's3://traceaudit/{str(os.path.basename(file_path))}'
        logger.warning(f"S3 URI-->> {file_uri}")
        # file_uri = "s3://traceaudit/C031-011.mp4"

        l = transcribe_file(f'my_joq_{time.time()}', file_uri, transcribe_client)
        logger.info(f"TRANSCRIPTION: {l}")
        if l:
            translated_text = GoogleTranslator(source='auto', target='en').translate(l)
            logger.info(f"TRANSLATED TEXT: {translated_text}")

            e = emotion_analyzer.predict(translated_text).probas
            h = hate_speech_analyzer.predict(translated_text).probas
            e = {k: v for k, v in sorted(e.items(), key=lambda item:- item[1])}
            h = {k: v for k, v in sorted(h.items(), key=lambda item:- item[1])}
            logger.info(f"EMOTION DATA:{e}")

            logger.info(f"HATE SPEECH DATA:{h}")
            return e, h
        else:
            logger.error(f"NO AUDIO FOUND")
            return {}, {}
    except Exception as ex:
        logger.error(f"FAILED TO GET THE AUDIO TRANSCRIPTION: {ex}")
        return {}, {}


if __name__=="__main__":
    file_path = "/home/saintadmin/work/video_analytics_tool/data/test_data/enghind.wav"
    if upload_video_for_transcribe(file_path):
        logger.info(f"FILE UPLOADED SUCCESSFULLY, PROCESSING WITH TRANSCRIPTION")

    file_uri = f's3://traceaudit/{str(os.path.basename(file_path))}'
    
    l = transcribe_file(f'my_joq_{time.time()}', file_uri, transcribe_client)

    logger.info(f"TRANSCRIPTION: {l}")
    if l:
        translated_text = GoogleTranslator(source='auto', target='en').translate(l)
        logger.info(f"TRANSLATED TEXT: {translated_text}")

        e = emotion_analyzer.predict(translated_text).probas
        h = hate_speech_analyzer.predict(translated_text).probas
        e = {k: v for k, v in sorted(e.items(), key=lambda item:- item[1])}
        h = {k: v for k, v in sorted(h.items(), key=lambda item:- item[1])}
        logger.info(f"EMOTION DATA:{e}")

        logger.info(f"HATE SPEECH DATA:{h}")
    else:
        logger.error(f"NO AUDIO FOUND")





