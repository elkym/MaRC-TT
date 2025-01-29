import os
import pickle
import json
from datetime import datetime

def save_hash_map(df_map, folder_path, timestamp, mrc_file_path, xlsx_file_path, update_metadata=True):
    file_path = os.path.join(folder_path, f"hash_map_{timestamp}.pkl")
    with open(file_path, 'wb') as f:
        pickle.dump(df_map, f)
    print(f"Hash map saved to {file_path}")

    if update_metadata:
        metadata = {
            'hash_map_file': f"hash_map_{timestamp}.pkl",
            'timestamp': timestamp,
            'mrc_file_path': mrc_file_path,
            'xlsx_file_path': xlsx_file_path 
        }
        metadata_file = os.path.join(folder_path, "hash_map_metadata.json")
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f)
        print(f"Metadata saved to {metadata_file}")

def load_hash_map(folder_path):
    try:
        metadata_file = os.path.join(folder_path, "hash_map_metadata.json")
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        file_path = os.path.join(folder_path, metadata['hash_map_file'])
        with open(file_path, 'rb') as f:
            df_map = pickle.load(f)
        print(f"Hash map loaded from {file_path}")
        return df_map
    except FileNotFoundError:
        print("No existing hash map found.")
        return None
    except KeyError as e:
        print(f"An unexpected error occurred: {e}")
        return None

def check_same_files(mrc_file_path, xlsx_file_path, folder_path):
    metadata_file = os.path.join(folder_path, "hash_map_metadata.json")
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        if 'mrc_file_path' in metadata and 'xlsx_file_path' in metadata:
            if metadata['mrc_file_path'] == mrc_file_path and metadata['xlsx_file_path'] == xlsx_file_path:
                return True
    return False

def save_metadata(mrc_file_path, xlsx_file_path, folder_path, timestamp):
    metadata = {
        'mrc_file_path': mrc_file_path,
        'xlsx_file_path': xlsx_file_path,
        'timestamp': timestamp,
        'hash_map_file': f"hash_map_{timestamp}.pkl"
    }
    metadata_file = os.path.join(folder_path, "hash_map_metadata.json")
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f)
    print(f"Metadata saved to {metadata_file}")