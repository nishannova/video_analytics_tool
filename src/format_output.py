
import pandas as pd
import json
import random
import datetime
from datetime import timedelta
import os

FINAL_OUTPUT_DIR = "/home/saintadmin/work/video_analytics_tool/data/processed/final_output"


def format_result(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        # print(data.keys())
    random_int = random.randint(100, 999999)   
    random_int2 = random.randint(0, random_int)
    social_media_channels = ["Facebook", "Twitter", "Instagram", "LinkedIn", "TikTok", "Snapchat"]
    cities = ["Mumbai", "Delhi", "Bengaluru", "Kolkata", "Chennai", "Hyderabad", "Pune"]
    date_list = ["1-Mar 8.24PM", "15-Jan 11.45AM", "21-Nov 6.30PM", "8-Sep 10.15AM", "12-May 9.00PM"]
    famous_list = ['Taj Mahal', '-', 'Virat Kohli', '-',  '-', '-', '-','Sachin Tendulkar', '-',   '-','Mumbai', 'Delhi',  '-','Jaipur', '-', 'Goa',  '-','Rajasthan', 'Kerala', 'Chennai', 'Kolkata', 'Bangalore', 'Hyderabad', 'Agra', 'Varanasi', 'Golden Temple', 'Jammu and Kashmir', 'Sundar Pichai', 'Indira Nooyi', 'Satya Nadella', 'Mukesh Ambani', 'Amitabh Bachchan', 'Shah Rukh Khan', 'Priyanka Chopra']
    social_media_list = ['@beautybeast', 'sundarlal', '@arrivinglate', '@foodieforever', '@wanderlust', '@fashionista', '@gamer4life', '@technerd', '@fitnessfreak', '@musiclover', '@bookworm', '@naturelover', '@petlover', '@movielover', '@photographer']
    reels_titles = [
    'Street Art Chronicles', 
    'Nature\'s Symphony', 
    'The Human Experience', 
    'Culinary Adventures', 
    'Cityscapes', 
    'Hidden Gems', 
    'Mindful Living', 
    'Behind the Scenes', 
    'Travel Diaries', 
    'Thrill Seekers', 
    'Sustainable Living', 
    'Everyday Heroes','We The People', 'For You', 'The Great Outdoors', 'Life in Slow Motion', 'Music is Life', 'Incredible India'
    ]
    

    random_date = random.choice(date_list)

    

    emotions = []
    # for i in range(len(data['AUDIO_AUDIT']['EMOTION_DETECTION'])):
    #     data['AUDIO_AUDIT']['EMOTION_DETECTION'][i][1] = {k:v for k,v in data['AUDIO_AUDIT']['EMOTION_DETECTION'][i][1].items() if k!='others' }
    #     data['AUDIO_AUDIT']['EMOTION_DETECTION'][i][2] = [x for x in data['AUDIO_AUDIT']['EMOTION_DETECTION'][i][1] if x]
    #     emotions.append(data['AUDIO_AUDIT']['EMOTION_DETECTION'][i][1])

    for em in data['AUDIO_AUDIT']['EMOTION_DETECTION']:
        print(f"MY EMOTIONS: {em[1]}")
        emotions.extend(em[1])

     
    data['AUDIO_AUDIT']['HATE_SPEECH_DETECTION'] = [x[1] for x in data['AUDIO_AUDIT']['HATE_SPEECH_DETECTION'] if x[1]]
    for i in data['AUDIO_AUDIT']['HATE_SPEECH_DETECTION']:
        emotions.extend(i)

    # emotions = [d for d in emotions if bool(d)]
    print(emotions)
    # emotions = [key for dictionary in emotions for key in dictionary.keys()]
    # print(emotions)
    # key_list = []
    random_channel = random.choice(social_media_channels)
    random_city = random.choice(cities)
    key_list =  [key for key in data["VIDEO_AUDIT"].keys()] 
    virality_score = (random_int2/ random_int) * 100
    selected_item = random.choice(famous_list)
    social_media_handle = random.choice(social_media_list)
    selected_title = random.choice(reels_titles)
    date_format = "%Y-%m-%d %H:%M:%S.%f"
    # print(f'[DEBUG] DATE TIME: {datetime.datetime.strptime(data.get("DateTime", str(datetime.datetime.now() - timedelta(days=365))), date_format)}')
    result = {
        "Title":os.path.basename(file_path).split("_data")[0],
        "CHANNEL":random_channel,
        "Handle":social_media_handle,
        "LOCATION":random_city,
        'EMOTION': data.get("AUDIO_AUDIT",{}).get("KEY_EMOTIONS",[]),
        'SITUATIONS':data.get("VIDEO_AUDIT", {}).get("KEY_SITUATIONS", []),
        "Virality":virality_score,
        "VIEWS":random_int,
        "REACTS":random_int2,
        "PErson/Location detected": selected_item ,
        "DateTime": datetime.datetime.strptime(data.get("DateTime", str(datetime.datetime.now() - timedelta(days=365))), date_format), #TODO: REMOVE DATETIME FROM HERE AND ADD IT TO PROCESSING
        "Threat": round( ( len(set(emotions)) + len(set(key_list)) ) / 12 * 100, 2 )
        }

    return result

def combine_results():
    results = list()
    for file in os.listdir(FINAL_OUTPUT_DIR):
        print(f"PROCESSING: {file}")
        results.append(format_result(os.path.join(FINAL_OUTPUT_DIR, file)))
    results.sort(key=lambda x: x["DateTime"], reverse=True)

    for idx, item in enumerate(results):
        results[idx]["DateTime"] = str(results[idx]["DateTime"])
    return json.dumps({"result": results})

if __name__ == "__main__":
    import pprint
    pprint.pprint(combine_results())