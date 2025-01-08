import os
import pandas as pd
import tkinter as tk
from file_handling import select_file, select_folder
from datetime import datetime
import config
import pymarc

def log_error(uid, title, error_code, error_message, additional_info=None):
    error_entry = {
        'UID/TN': uid,
        'Title': title,
        'Error Code': error_code,
        'Error Message': error_message
    }
    if additional_info:
        error_entry.update(additional_info)
    error_entries.append(error_entry)

def get_title(record):
    if '245' in record and 'a' in record['245']:
        return record['245']['a']
    return 'Title not found'

def get_last_modified_time(file_path):
    """
    Gets the last modified time of a file.

    Parameters:
    - file_path: The path to the file.

    Returns:
    - A string representing the last modified time in a readable format.
    """
    timestamp = os.path.getmtime(file_path)
    last_modified_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return last_modified_time

# Main script
try:
    # Select MARC file
    mrc_file_path = select_file("*.mrc")

    # Select Tabular file
    xlsx_file_path = select_file("*.xlsx", "*.parquet")

    marc_last_modified = get_last_modified_time(mrc_file_path)
    xlsx_last_modified = get_last_modified_time(xlsx_file_path)

    marc_records = {}
    with open(mrc_file_path, 'rb') as fh:
        reader = pymarc.MARCReader(fh)
        for record in reader:
            uid = record['001'].value()
            marc_records[uid] = record

    print("MARC records loaded successfully.")

    error_entries = []

    # Use max_repeats from config file
    max_repeats = config.MAX_REPEATS

    for uid, record in marc_records.items():
        field_counts = {}
        for field in record.fields:
            if field.tag not in field_counts:
                field_counts[field.tag] = 0
            field_counts[field.tag] += 1
        for field_tag, count in field_counts.items():
            if count > max_repeats:
                log_error(uid, get_title(record), 'ERR008', f'Excessive repetitions of field {field_tag}', {'Field': field_tag, 'Count': count})

    error_log = pd.DataFrame(error_entries)
    
    # Prepend the error log with a line indicating the files used and their last modified times
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    header_info = (
        f"Files used: MARC file - {mrc_file_path} (Last modified: {marc_last_modified}), "
        f"XLSX file - {xlsx_last_modified} (Last modified: {xlsx_last_modified})\n"
        f"Timestamp: {timestamp}\n"
    )
    
    # Extract the base name of the MARC file (without extension)
    base_name = os.path.splitext(os.path.basename(mrc_file_path))[0]

    # Select folder to save the error log
    folder_path = select_folder()
    print(f"Selected folder: {folder_path}")

    # Update the output file name
    output_file_path = os.path.join(folder_path, f"{base_name}_error_log.tsv")

    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(header_info)
        error_log.to_csv(f, sep='\t', index=False, encoding='utf-8')
    
    print(f"Field repetition analysis and error logging completed successfully. Log saved to {output_file_path}")

except FileNotFoundError as e:
    print(e)
except Exception as e:
    print(f"An unexpected error occurred: {e}")