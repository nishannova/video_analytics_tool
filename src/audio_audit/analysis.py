from __future__ import print_function
import os
import boto3
import time
import urllib
import json
from deep_translator import GoogleTranslator
from pysentimiento import create_analyzer
from loguru import logger
import json
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
                return data
            break
        else:
            print(f"Waiting for {job_name}. Current status is {job_status}.")
        time.sleep(10)




def get_word_timestamps(transcript):
    word_timestamps = []
    for result in transcript['results']:
        for item in result['alternatives'][0]['items']:
            if item['type'] == 'pronunciation':
                word_timestamps.append(float(item['start_time']))
    return word_timestamps

# def process_audio(file_path, segment_duration=5):
#     try:
#         if upload_video_for_transcribe(file_path):
#             logger.info(f"FILE UPLOADED SUCCESSFULLY, PROCESSING WITH TRANSCRIPTION")
#         file_uri = f's3://traceaudit/{str(os.path.basename(file_path))}'
#         logger.warning(f"S3 URI-->> {file_uri}")

#         transcript = transcribe_file(f'my_joq_{time.time()}', file_uri, transcribe_client)
#         if transcript:
#             l = transcript['results']['transcripts'][0]['transcript']
#             logger.info(f"TRANSCRIPTION: {l}")

#             # Extract word-level timestamps
#             items = transcript['results']['items']
#             word_timestamps = [(item['alternatives'][0]['content'], float(item['start_time'])) for item in items if item.get('start_time')]

#             # Group words into segments based on segment_duration
#             segments = []
#             segment = []
#             start_time = 0
#             for word, timestamp in word_timestamps:
#                 if timestamp - start_time < segment_duration:
#                     segment.append(word)
#                 else:
#                     segments.append((start_time, ' '.join(segment)))
#                     start_time += segment_duration
#                     segment = [word]
#             if segment:
#                 segments.append((start_time, ' '.join(segment)))

#             # Analyze emotions and hate speech for each segment
#             emotion_data = []
#             hate_speech_data = []
#             for start_time, segment_text in segments:
#                 translated_segment = GoogleTranslator(source='auto', target='en').translate(segment_text)

#                 e = emotion_analyzer.predict(translated_segment).probas
#                 h = hate_speech_analyzer.predict(translated_segment).probas
#                 e = {k: v for k, v in sorted(e.items(), key=lambda item:- item[1])}
#                 h = {k: v for k, v in sorted(h.items(), key=lambda item:- item[1])}

#                 emotion_data.append((start_time, e))
#                 hate_speech_data.append((start_time, h))

#             logger.info(f"EMOTION DATA WITH TIMESTAMPS:{emotion_data}")
#             logger.info(f"HATE SPEECH DATA WITH TIMESTAMPS:{hate_speech_data}")

#             return emotion_data, hate_speech_data
#         else:
#             logger.error(f"NO AUDIO FOUND")
#             return [], []
#     except Exception as ex:
#         logger.error(f"FAILED TO GET THE AUDIO TRANSCRIPTION: {ex}")
#         return [], []



# def process_audio(file_path):
#     try:
#         if upload_video_for_transcribe(file_path):
#             logger.info(f"FILE UPLOADED SUCCESSFULLY, PROCESSING WITH TRANSCRIPTION")
#         file_uri = f's3://traceaudit/{str(os.path.basename(file_path))}'
#         logger.warning(f"S3 URI-->> {file_uri}")

#         transcript = transcribe_file(f'my_joq_{time.time()}', file_uri, transcribe_client)
#         if transcript:
#             l = transcript['results']['transcripts'][0]['transcript']
#             logger.info(f"TRANSCRIPTION: {l}")

#             # Extract word-level timestamps
#             items = transcript['results']['items']
#             word_timestamps = [(item['alternatives'][0]['content'], float(item['start_time'])) for item in items if item.get('start_time')]

#             # Translate the entire transcript
#             translated_transcript = GoogleTranslator(source='auto', target='en').translate(l)

#             # Analyze emotions and hate speech for the entire transcript
#             e = emotion_analyzer.predict(translated_transcript).probas
#             h = hate_speech_analyzer.predict(translated_transcript).probas
#             e = {k: v for k, v in sorted(e.items(), key=lambda item: -item[1]) if v>0.3}
#             h = {k: v for k, v in sorted(h.items(), key=lambda item: -item[1]) if v>0.3}

#             # Assign the same word timestamps for each category
#             emotion_data = {category: word_timestamps for category in e}
#             hate_speech_data = {category: word_timestamps for category in h}

#             logger.info(f"EMOTION DATA WITH TIMESTAMPS:{emotion_data}")
#             logger.info(f"HATE SPEECH DATA WITH TIMESTAMPS:{hate_speech_data}")

#             return emotion_data, hate_speech_data

#         else:
#             logger.error(f"NO AUDIO FOUND")
#             return {}, {}
#     except Exception as ex:
#         logger.error(f"FAILED TO GET THE AUDIO TRANSCRIPTION: {ex}")
#         return {}, {}



def process_audio(file_path, segment_duration=5):
    try:
        if upload_video_for_transcribe(file_path):
            logger.info(f"FILE UPLOADED SUCCESSFULLY, PROCESSING WITH TRANSCRIPTION")
        file_uri = f's3://traceaudit/{str(os.path.basename(file_path))}'
        logger.warning(f"S3 URI-->> {file_uri}")

        transcript = transcribe_file(f'my_joq_{time.time()}', file_uri, transcribe_client)
        if transcript:
            l = transcript['results']['transcripts'][0]['transcript']
            logger.info(f"TRANSCRIPTION: {l}")

            # Extract word-level timestamps
            items = transcript['results']['items']
            word_timestamps = [(item['alternatives'][0]['content'], float(item['start_time'])) for item in items if item.get('start_time')]

            # Group words into segments based on segment_duration
            segments = []
            segment = []
            start_time = 0
            for word, timestamp in word_timestamps:
                if timestamp - start_time < segment_duration:
                    segment.append((word, timestamp))
                else:
                    segments.append((start_time, segment))
                    start_time += segment_duration
                    segment = [(word, timestamp)]
            if segment:
                segments.append((start_time, segment))

            # Analyze emotions and hate speech for each segment
            emotion_data = []
            hate_speech_data = []
            for start_time, segment in segments:
                segment_text = ' '.join([word for word, _ in segment])
                translated_segment = GoogleTranslator(source='auto', target='en').translate(segment_text)

                e = emotion_analyzer.predict(translated_segment).probas
                h = hate_speech_analyzer.predict(translated_segment).probas
                e = {k: v for k, v in sorted(e.items(), key=lambda item:- item[1]) if v > 0.3}
                h = {k: v for k, v in sorted(h.items(), key=lambda item:- item[1]) if v > 0.3}

                emotion_data.append((start_time, e, segment))
                hate_speech_data.append((start_time, h, segment))

            logger.info(f"EMOTION DATA WITH TIMESTAMPS:{emotion_data}")
            logger.info(f"HATE SPEECH DATA WITH TIMESTAMPS:{hate_speech_data}")

            return emotion_data, hate_speech_data
        else:
            logger.error(f"NO AUDIO FOUND")
            return [], []
    except Exception as ex:
        logger.error(f"FAILED TO GET THE AUDIO TRANSCRIPTION: {ex}")
        return [], []
