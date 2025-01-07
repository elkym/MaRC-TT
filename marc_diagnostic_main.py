import pandas as pd
import tkinter as tk
from tkinter import ttk
from file_handling import select_file
from data_transformation import (
    escape_line_breaks, flatten_list, generate_column_names, 
    extract_field_codes, sort_columns, extract_indicators, 
    filter_records, marc_to_dataframe, custom_write, save_to_xlsxwriter_in_chunks
)

# Function to create Tkinter window (field dropper)
def create_selection_window(fields):
    global fields_to_drop  # Declare global variable
    root = tk.Tk()
    root.title("Select Fields to Drop")

    # Create a frame to hold checkboxes
    frame = ttk.Frame(root, padding="10")
    frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    # Create a dictionary to hold checkbox variables
    field_vars = {field: tk.BooleanVar() for field in fields}

    # Create checkboxes in 12 columns
    columns = 12
    for i, field in enumerate(sorted(fields)):
        row = i // columns
        col = i % columns
        checkbox = ttk.Checkbutton(frame, text=field, variable=field_vars[field])
        checkbox.grid(row=row, column=col, sticky=tk.W, padx=5, pady=5)

    # Function to handle OK button click
    def on_ok():
        global fields_to_drop  # Declare global variable
        fields_to_drop = [field for field, var in field_vars.items() if var.get()]
        print("Selected fields to drop:", fields_to_drop)
        root.destroy()

    # Add OK button
    ok_button = ttk.Button(frame, text="OK", command=on_ok)
    ok_button.grid(row=(len(fields) // columns) + 1, column=0, columnspan=columns, pady=10)

    root.mainloop()

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