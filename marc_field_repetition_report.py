import os
import pandas as pd
from pymarc import MARCReader
from file_handling import select_file
import config
from datetime import datetime

# Function to log errors
def log_error(uid, title, field_tag, subfield_code, count):
    if uid not in error_entries:
        error_entries[uid] = {
            'UID/TN': uid,
            'Title': title
        }
    if f'Count_{field_tag}_{subfield_code}' not in error_entries[uid]:
        error_entries[uid][f'Count_{field_tag}_{subfield_code}'] = 0
    error_entries[uid][f'Count_{field_tag}_{subfield_code}'] += count

# Function to get the title from a MARC record
def get_title(record):
    if '245' in record and 'a' in record['245']:
        return record['245']['a']
    return 'Title not found'

# Function to get the last modified time of a file
def get_last_modified_time(file_path):
    timestamp = os.path.getmtime(file_path)
    last_modified_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return last_modified_time

# Starting file selection process
print("Starting file selection process...")
mrc_file_path = select_file("*.mrc")

# Automatically find the matching error log file
base_name = os.path.splitext(mrc_file_path)[0]
error_log_file_path = f"{base_name}_error_log.tsv"

# Load MARC records into a dictionary
print("Reading MARC records...")
marc_records = {}
try:
    with open(mrc_file_path, 'rb') as fh:
        reader = MARCReader(fh)
        for record in reader:
            uid = record['001'].value()
            marc_records[uid] = record
    print("MARC records loaded successfully.")
except Exception as e:
    print(f"Error: Unable to read the MARC file. The script will now terminate. {e}")
    exit(1)

# Load error log data into a pandas DataFrame, skipping the first two lines
print("Loading Main Error Log data...")
try:
    error_log = pd.read_csv(error_log_file_path, sep='\t', dtype=str, skiprows=2)
    print("Main Error Log data loaded successfully.")
except Exception as e:
    print(f"Error: Unable to read the Main Error Log file. The script will now terminate. {e}")
    exit(1)

# Create a list of fields to examine from the Main Error Log
fields_to_examine = error_log['Field'].unique().tolist()

# Initialize the error log dictionary
error_entries = {}

# Use max_repeats from config.py
max_repeats = config.MAX_REPEATS

# Analyze field repetitions and log errors
print("Analyzing field repetitions...")
for uid, record in marc_records.items():
    for field_tag in fields_to_examine:
        if field_tag in record:
            subfield_counts = {}
            for field in record.get_fields(field_tag):
                for subfield_code, subfield_value in field.subfields_as_dict().items():
                    if subfield_value:  # Check if subfield is not empty
                        if subfield_code not in subfield_counts:
                            subfield_counts[subfield_code] = 0
                        subfield_counts[subfield_code] += 1
            
            for subfield_code, count in subfield_counts.items():
                if count > max_repeats:
                    log_error(uid, get_title(record), field_tag, subfield_code, count)

print("Field repetition analysis completed.")

# Convert error entries to a DataFrame
error_log_df = pd.DataFrame.from_dict(error_entries, orient='index')

# Save the error log as a TSV file with UTF-8 encoding
output_folder_path = os.path.dirname(error_log_file_path)
output_base_name = os.path.splitext(os.path.basename(error_log_file_path))[0]
output_file_path = os.path.join(output_folder_path, f"{output_base_name}_repetition_analysis.tsv")

print("Saving Repeated Fields Analysis log to TSV file...")
error_log_df.to_csv(output_file_path, sep='\t', index=False, encoding='utf-8')
print("Repeated Fields Analysis log saved successfully.")