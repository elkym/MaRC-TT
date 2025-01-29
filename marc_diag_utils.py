import pymarc
import config
from tqdm import tqdm
import random
import pandas as pd
import numpy as np

# Function to load MARC records
def load_marc_records(mrc_file_path):
    marc_records = {}
    with open(mrc_file_path, 'rb') as fh:
        reader = pymarc.MARCReader(fh)
        for record in reader:
            uid = record['001'].value()
            marc_records[uid] = record
    return marc_records

# Function to get title from MARC record
def get_title(record):
    if '245' in record and 'a' in record['245']:
        return record['245']['a']
    return 'Title not found'

# Function to generate diagnostic log
def diag_log(uid, title, error_code, error_message, marc_data=None, tabular_data=None, additional_info=None):
    error_entry = {
        'UID/TN': uid,
        'Title': title,
        'Error Code': error_code,
        'Error Message': error_message
    }
    if marc_data is not None and isinstance(marc_data, str):
        error_entry['MaRC_data'] = marc_data.replace('\n', '\\n')
    if tabular_data is not None and isinstance(tabular_data, str):
        error_entry['Tabular_data'] = tabular_data.replace('\n', '\\n')
    if additional_info:
        error_entry.update(additional_info)
    return error_entry

# Function to check for non-digit characters in UID
def check_non_digit_uid(uid):
    return not str(uid).isdigit()

# Function to check for non-UTF-8 encoding
def check_non_utf8_encoding(value):
    try:
        str(value).encode('utf-8')
    except UnicodeEncodeError:
        return True
    return False

# Function to check for records not found in Excel
def check_records_not_found(
        df_map, repetitions_dict,
        marc_records, error_entries): # error_entries is a variable in the marc_diagnostic_main script
    for uid in list(repetitions_dict.keys()):
        if uid in df_map:
            del repetitions_dict[uid]
        else:
            repetitions_dict[uid].append(diag_log(uid, get_title(marc_records[uid]), config.ERR_RECORD_NOT_FOUND, "Record not found in Excel"))

# Function to compare strings (case-insensitive)
def compare_strings(str1, str2):
    if str1 is None or str2 is None:
        return str1 == str2
    else:
        return str1.lower() == str2.lower()

# Function to create hash map
def create_hash_map(df):
    df_map = {}
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Creating hash map"):
        uid = row['001.1.']
        df_map[uid] = row.replace({pd.NA: '', np.nan: ''})  # Replace NaN values with empty string
    return df_map

def perform_checks(df, df_map, repetitions_dict, error_entries, marc_records):
    # Check for duplicate control fields
    cols_to_check = 20
    control_fields = [col[:3] for col in df.columns[:cols_to_check] if col[:3] in config.CONTROL_FIELDS]
    print(f"Control fields to check: {control_fields}")  # Debug statement

    # Count occurrences of each control field
    field_counts = {}
    for field in control_fields:
        if field not in field_counts:
            field_counts[field] = 0
        field_counts[field] += 1

    print(f"Field counts: {field_counts}")  # Debug statement

    # Check for duplicates
    duplicates = [field for field, count in field_counts.items() if count > 1]
    if duplicates:
        duplicate_field = duplicates[0]
        print(f"Duplicate control field found: {duplicate_field}")  # Debug statement
        print(f"Control fields set: {set(control_fields)}")
        print(f"Expected control fields: {config.CONTROL_FIELDS}")
        
        proceed = input("Diagnostic has found duplicate control fields in tabular data. Do you wish to proceed anyway? (y/n): ")
        if proceed.lower() != 'y':
            print("Aborting the script due to duplicate control fields.")
            return
        
        error_entries.append(diag_log(
            None,  # UID not applicable here
            None,  # Title not applicable here
            config.ERR_DUPLICATE_CONTROL_FIELD,
            f"Duplicate control field found: {duplicate_field}"
        ))
        print(f"Error: Duplicate control field found: {duplicate_field}")  # Debug statement
    
    for uid, row in df_map.items():
        if check_non_digit_uid(uid):
            error_entries.append(diag_log(uid, row['245.1.a'], config.ERR_NON_DIGIT_UID, "Non-digit characters in UID"))

        for col in row.index:
            value = row[col]
            if check_non_utf8_encoding(value):
                error_entries.append(diag_log(uid, row['245.1.a'], config.ERR_NON_UTF8_ENCODING, "Non-UTF-8 encoding detected", {'Field': col}))

    check_records_not_found(df_map, repetitions_dict, marc_records, error_entries)
    print(f"Error entries after perform_checks: {error_entries}")  # Debug statement

# Function to analyze field repetitions and log errors in a dictionary with UID as the key
def analyze_field_repetitions(marc_records, max_repeats):
    repetitions_dict = {}
    for uid, record in marc_records.items():
        field_counts = {}
        for field in record.fields:
            if field.tag not in field_counts:
                field_counts[field.tag] = 0
            field_counts[field.tag] += 1
        for field_tag, count in field_counts.items():
            if count > max_repeats:
                if uid not in repetitions_dict:
                    repetitions_dict[uid] = []
                repetitions_dict[uid].append(diag_log(
                    uid,
                    get_title(record),
                    config.ERR_EXCESSIVE_FIELD_REPETITIONS,
                    f"Field {field_tag} repeated {count} times",
                    {'Field': field_tag, 'Repetitions': count}
                ))
    return repetitions_dict

def validate_and_sample_check(df_map, sample_fraction=0.1, early_stop=True):
    # Check if df_map is empty
    if not df_map:
        print("df_map is empty.")
        return False

    # Random sampling check
    sample_size = int(len(df_map) * sample_fraction)
    sampled_uids = random.sample(list(df_map.keys()), sample_size)

    for uid in sampled_uids:
        row = df_map[uid]

        # UID Check
        if check_non_digit_uid(uid):
            error_entries.append(diag_log(uid, row['245.1.a'], config.ERR_NON_DIGIT_UID, "Non-digit characters in UID")) # error_entries is a variable in the marc_diagnostic_main script
            if early_stop:
                print("Error detected, stopping early.")
                return False

        # Encoding Check
        for col in row.index:
            value = row[col]
            if check_non_utf8_encoding(value):
                error_entries.append(diag_log(uid, row['245.1.a'], config.ERR_NON_UTF8_ENCODING, "Non-UTF-8 encoding detected", {'Field': col})) # error_entries is a variable in the marc_diagnostic_main script
                if early_stop:
                    print("Error detected, stopping early.")
                    return False
    return True
