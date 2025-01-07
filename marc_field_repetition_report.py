import pandas as pd
import pymarc
from datetime import datetime
from tkinter import Tk, filedialog

# Function to select files using tkinter dialog
def select_file(file_type, file_extension):
    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(
        title=f"Select {file_type} file",
        filetypes=[(file_type, file_extension)]
    )
    if not file_path:
        raise FileNotFoundError(f"No {file_type} file selected.")
    return file_path

# Select the .mrc and error_log.tsv files using tkinter dialog
mrc_file_path = select_file("MARC", "*.mrc")
error_log_file_path = select_file("Error Log", "*.tsv")

# Read MARC records into a dictionary
marc_records = {}
with open(mrc_file_path, 'rb') as fh:
    reader = pymarc.MARCReader(fh)
    for record in reader:
        uid = record['001'].value()
        marc_records[uid] = record

# Load error log data into a pandas DataFrame
error_log = pd.read_csv(error_log_file_path, sep='\t', dtype=str)

print("MARC records and error log loaded successfully.")

# Function to log errors
def log_error(uid, title, error_code, additional_info=None):
    error_entry = {
        'Timestamp': timestamp,
        'UID/TN': uid,
        'Title': title,
        'Error Code': error_code
    }
    if additional_info:
        error_entry.update(additional_info)
    error_entries.append(error_entry)

# Function to get the title from a MARC record
def get_title(record):
    if '245' in record and 'a' in record['245']:
        return record['245']['a']
    return 'Title not found'

# Initialize the error log DataFrame with a timestamp
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
error_entries = []

# Define max_repeats (assuming it's defined in the flattening script)
max_repeats = 15

# Count field repetitions and log errors
for uid, record in marc_records.items():
    field_counts = {}
    for field in record.fields:
        if field.tag not in field_counts:
            field_counts[field.tag] = 0
        field_counts[field.tag] += 1
    for field_tag, count in field_counts.items():
        if count > max_repeats:
            log_error(uid, get_title(record), f'ERR008: Excessive repetitions of field {field_tag}', {'Field': field_tag, 'Count': count})

# Append all error entries to the error log DataFrame
error_log = pd.concat([error_log, pd.DataFrame(error_entries)], ignore_index=True)

# Save the error log as a TSV file with UTF-8 encoding
error_log.to_csv('field_repetition_error_log.tsv', sep='\t', index=False, encoding='utf-8')

print("Field repetition analysis and error logging completed successfully.")