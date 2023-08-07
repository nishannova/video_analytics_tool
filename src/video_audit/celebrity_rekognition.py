
import os
import cv2
import numpy as np
import faiss
import pandas as pd
from deepface import DeepFace
import ast
# Function to normalize embeddings
def normalize_embeddings(embeddings):
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    return embeddings / norms

# Function to create a Faiss index for cosine similarity
def create_faiss_index_cosine(csv_file_path):
        df = pd.read_csv(csv_file_path)
        embeddings = np.array(df['Embedding'].apply(ast.literal_eval).values.tolist(),dtype='float32')
        normalized_embeddings = normalize_embeddings(embeddings)
        index = faiss.IndexFlatIP(normalized_embeddings.shape[1])
        index.add(normalized_embeddings)
        return index

# def find_closest_celebrities_cosine(video_path, csv_file_path,output_folder, faiss_index, threshold=0.7, save_frames=True):
    
    
#     # Function to get the timestamp from the frame number and video's FPS
#     def get_timestamp(frame_number, fps):
#         return round(frame_number / fps, 3)  # Round off to 3 decimal points

#     # Load the CSV file with celebrity information
#     df = pd.read_csv(csv_file_path)

#     # Initialize variables
#     cap = cv2.VideoCapture(video_path)
#     fps = cap.get(cv2.CAP_PROP_FPS)
#     closest_celebrities_dict = {}  # To store the closest celebrities and their timestamps

#     # Filter frames and process only the desired ones
#     frames_to_process = [frame_number for frame_number in range(int(cap.get(cv2.CAP_PROP_FRAME_COUNT))) if frame_number % 120 == 0]

#     for frame_number in frames_to_process:
#         cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
#         ret, frame = cap.read()
#         if not ret:
#             break

#         # Calculate face embeddings for the frame
#         embedding_objs = DeepFace.represent(frame, enforce_detection=False, model_name='Facenet512')

#         # Initialize a dictionary to store the closest celebrities for the current frame with their timestamps
#         frame_celebrities = {}

#         if embedding_objs:
#             for embedd in embedding_objs:
#                 input_embedding = np.array(embedd['embedding'], dtype='float32')
#                 input_embedding = input_embedding / np.linalg.norm(input_embedding)  # Normalize the input embedding

#                 # Find the k-nearest neighbors using Faiss
#                 k = 1
#                 distances, indices = faiss_index.search(np.expand_dims(input_embedding, axis=0), k)
#                 similarity = distances[0][0]  # Since k=1, take the first element of distances array

#                 if similarity < threshold:
#                     closest_celebrity = "Unknown"
#                 else:
#                     closest_celebrity = df.iloc[indices[0][0]]['Name']

#                 # Add the closest celebrity to the dictionary for the current frame with timestamp
#                 timestamp = get_timestamp(frame_number, fps)

#                 # Update the closest_celebrities_dict
#                 if closest_celebrity in closest_celebrities_dict:
#                     closest_celebrities_dict[closest_celebrity].append(timestamp)
#                 else:
#                     closest_celebrities_dict[closest_celebrity] = [timestamp]

#                 # Draw bounding box and label on the frame (optional)
#                 text = f"{closest_celebrity}"
#                 cv2.putText(frame, f'{text}', (embedd['facial_area']['x'], embedd['facial_area']['y'] - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.3, (0, 255, 0), 1)
#                 cv2.rectangle(frame, (embedd['facial_area']['x'], embedd['facial_area']['y']), (embedd['facial_area']['x']+embedd['facial_area']['w'], embedd['facial_area']['y']+embedd['facial_area']['h']), (0, 255, 0), 2)

#                 # Save frame if save_frames is True and celebrity is recognized (optional)
#                 if save_frames and closest_celebrity != "Unknown":
#                     os.makedirs(os.path.join(output_folder, closest_celebrity), exist_ok=True)
#                     output_frame_path = os.path.join(output_folder, closest_celebrity, f'frame_{frame_number}.jpg')
#                     cv2.imwrite(output_frame_path, frame)

#     cap.release()
#     # Invert the dictionary to have celebrity names as keys and their timestamps as values
#     inverted_dict = {}
#     for celeb, timestamps in closest_celebrities_dict.items():
#         inverted_dict[celeb] = {"Timestamp": timestamps}

#     # Return the dictionary with celebrity names as keys and their timestamps as values
#     return inverted_dict
#     # Return the dictionary with celebrity names as keys and their timestamps as values
#     # return closest_celebrities_dict





def find_closest_celebrities_cosine(video_path, csv_file_path,output_folder, faiss_index, threshold=0.8, save_frames=False):
    # Function to get the timestamp from the frame number and video's FPS
    def get_timestamp(frame_number, fps):
        return round(frame_number / fps, 3)  # Round off to 3 decimal points

    # Load the CSV file with celebrity information
    df = pd.read_csv(csv_file_path)
    video_name = video_path.split('/')[-1].split('.')[0]
    os.makedirs(os.path.join(output_folder, video_name), exist_ok=True)
    path_to_save_frame = os.path.join(output_folder, video_name)
    # Initialize variables
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    closest_celebrities_dict = {}  # To store the closest celebrities and their timestamps

    # Filter frames and process only the desired ones
    frames_to_process = [frame_number for frame_number in range(int(cap.get(cv2.CAP_PROP_FRAME_COUNT))) if frame_number % 70 == 0]

    for frame_number in frames_to_process:
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
        ret, frame = cap.read()
        if not ret:
            break

        # Calculate face embeddings for the frame
        embedding_objs = DeepFace.represent(frame, enforce_detection=False, model_name='Facenet512')

        # Initialize a dictionary to store the closest celebrities for the current frame with their timestamps
        frame_celebrities = {}

        if embedding_objs:
            for embedd in embedding_objs:
                input_embedding = np.array(embedd['embedding'], dtype='float32')
                input_embedding = input_embedding / np.linalg.norm(input_embedding)  # Normalize the input embedding

                # Find the k-nearest neighbors using Faiss
                k = 1
                distances, indices = faiss_index.search(np.expand_dims(input_embedding, axis=0), k)
                similarity = distances[0][0]  # Since k=1, take the first element of distances array

                if similarity < threshold:
                    closest_celebrity = "Unknown"
                else:
                    closest_celebrity = df.iloc[indices[0][0]]['Name']
                    closest_celebrity_category = df.iloc[indices[0][0]]['Category']
                # Add the closest celebrity to the dictionary for the current frame with timestamp
                timestamp = get_timestamp(frame_number, fps)

                # Update the closest_celebrities_dict
                if closest_celebrity in closest_celebrities_dict:
                    closest_celebrities_dict[closest_celebrity].append(timestamp)
                # elif closest_celebrity == 'Unknown':
                #     closest_celebrities_dict[(closest_celebrity,'Unknown')].append(timestamp)
                else:
                    closest_celebrities_dict[closest_celebrity] = [timestamp]

                # Draw bounding box and label on the frame (optional)
                text = f"{closest_celebrity}"
                cv2.putText(frame, f'{text}', (embedd['facial_area']['x'], embedd['facial_area']['y'] - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.3, (0, 255, 0), 1)
                cv2.rectangle(frame, (embedd['facial_area']['x'], embedd['facial_area']['y']), (embedd['facial_area']['x']+embedd['facial_area']['w'], embedd['facial_area']['y']+embedd['facial_area']['h']), (0, 255, 0), 2)

                # Save frame if save_frames is True and celebrity is recognized (optional)
                if save_frames and closest_celebrity != "Unknown":
                    os.makedirs(os.path.join(path_to_save_frame, closest_celebrity), exist_ok=True)
                    output_frame_path = os.path.join(path_to_save_frame, closest_celebrity, f'frame_{frame_number}.jpg')
                    cv2.imwrite(output_frame_path, frame)

    cap.release()
    inverted_dict = {}
    # closest_celebrities_dict =  {':'.join(k): v for k, v in closest_celebrities_dict.items()}
    for celeb, timestamps in closest_celebrities_dict.items():
        inverted_dict[celeb] = {"Timestamp": timestamps}
    # Return the dictionary with celebrity names as keys and their timestamps as values
    return inverted_dict
    
