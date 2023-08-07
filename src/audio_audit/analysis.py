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
import math
from translate import Translator

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


#OLD FUNCTION
# def process_audio(file_path, duration):
#     try:
#         segment_duration = math.ceil(duration / 10)
#         if upload_video_for_transcribe(file_path):
#             logger.info(f"FILE UPLOADED SUCCESSFULLY, PROCESSING WITH TRANSCRIPTION")
#         file_uri = f's3://traceaudit/{str(os.path.basename(file_path))}'
#         logger.warning(f"S3 URI-->> {file_uri}")

#         transcript = transcribe_file(f'my_joq_{time.time()}', file_uri, transcribe_client)
#         if transcript:
#             l = transcript['results']['transcripts'][0]['transcript']
#             language_code = transcript['results']['language_code']
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
#                     segment.append((word, timestamp))
#                 else:
#                     segments.append((start_time, segment))
#                     start_time += segment_duration
#                     segment = [(word, timestamp)]
#             if segment:
#                 segments.append((start_time, segment))

#             # Analyze emotions and hate speech for each segment
#             emotion_data = []
#             hate_speech_data = []
#             translation = str()
#             for start_time, segment in segments:
#                 segment_text = ' '.join([word for word, _ in segment])
#                 translated_segment = GoogleTranslator(source='auto', target='en').translate(segment_text)
#                 translation +=  " " + translated_segment
#                 e = emotion_analyzer.predict(translated_segment).probas
#                 h = hate_speech_analyzer.predict(translated_segment).probas
#                 print(f"RAW HATE: {h}")
#                 e = {k: v for k, v in sorted(e.items(), key=lambda item:- item[1]) if v > 0.3 and k != "others"}
#                 h = {k: v for k, v in sorted(h.items(), key=lambda item:- item[1]) if v > 0.3}
#                 if e:
#                     emotion_data.append((start_time, list(e.keys()), list(e.values()), " ".join([word[0] for word in segment])))
#                 if h:
#                     hate_speech_data.append((start_time, list(h.keys()), list(e.values()), " ".join([word[0] for word in segment])))

#             logger.info(f"EMOTION DATA WITH TIMESTAMPS:{emotion_data}")
#             logger.info(f"HATE SPEECH DATA WITH TIMESTAMPS:{hate_speech_data}")

#             return emotion_data, hate_speech_data, l, translation, language_code, segment_duration
#         else:
#             logger.error(f"NO AUDIO FOUND")
#             return [], [], "", "", "", segment_duration
#     except Exception as ex:
#         logger.error(f"FAILED TO GET THE AUDIO TRANSCRIPTION: {ex}")
#         return [], [], "", "", "", segment_duration

from deep_translator import GoogleTranslator

def translate_in_chunks(text, target='en', chunk_size=4000):
    translated_text = ""

    # Split text into chunks of specified size
    chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]
    for chunk in chunks:
        # translated_chunk = GoogleTranslator(source='auto', target=target).translate(chunk)
        # translated_text += translated_chunk
        translator= Translator(to_lang="en")
        translated_text += translator.translate(chunk)

    return translated_text

# # Use it like this:
# long_text = "Your long text here"
# translated_text = translate_in_chunks(long_text, target='en')

def process_audio(file_path, duration):
    try:
        segment_duration = math.ceil(duration / 10)
        if upload_video_for_transcribe(file_path):
            logger.info(f"FILE UPLOADED SUCCESSFULLY, PROCESSING WITH TRANSCRIPTION")
        file_uri = f's3://traceaudit/{str(os.path.basename(file_path))}'
        logger.warning(f"S3 URI-->> {file_uri}")

        transcript = transcribe_file(f'my_joq_{time.time()}', file_uri, transcribe_client)
        if transcript:
            l = transcript['results']['transcripts'][0]['transcript']
            language_code = transcript['results']['language_code']
            logger.info(f"TRANSCRIPTION: {l} with a length: {len(l)}")

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
            translation = str()
            for start_time, segment in segments:
                segment_text = ' '.join([word for word, _ in segment])
                logger.debug(f"CALLING TRANSLATE WITH SEGMENT: {segment_text}")
                translated_segment = translate_in_chunks(segment_text, target='en') # Using the function defined earlier
                translation +=  " " + translated_segment
                e = emotion_analyzer.predict(translated_segment).probas
                h = hate_speech_analyzer.predict(translated_segment).probas
                print(f"RAW HATE: {h}")
                e = {k: v for k, v in sorted(e.items(), key=lambda item:- item[1]) if v > 0.3 and k != "others"}
                h = {k: v for k, v in sorted(h.items(), key=lambda item:- item[1]) if v > 0.3}
                if e:
                    emotion_data.append((start_time, list(e.keys()), list(e.values()), " ".join([word[0] for word in segment])))
                if h:
                    hate_speech_data.append((start_time, list(h.keys()), list(h.values()), " ".join([word[0] for word in segment])))

            logger.info(f"EMOTION DATA WITH TIMESTAMPS:{emotion_data}")
            logger.info(f"HATE SPEECH DATA WITH TIMESTAMPS:{hate_speech_data}")

            return emotion_data, hate_speech_data, l, translation, language_code, segment_duration
        else:
            logger.error(f"NO AUDIO FOUND")
            return [], [], "", "", "", segment_duration
    except Exception as ex:
        logger.error(f"FAILED TO GET THE AUDIO TRANSCRIPTION: {ex}")
        return [], [], "", "", "", segment_duration
