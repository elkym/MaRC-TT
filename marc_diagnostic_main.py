import dask.dataframe as dd
import pandas as pd
import os
import config
from data_transformation import get_title, load_marc_records
from file_handling import select_file_to_open, select_folder
import logging

# Configure the logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(r'C:\Users\elkym\OneDrive - Church of Jesus Christ\Documents\MarcTT-2025-Testing\dasktests_error_log.log'),
                        logging.StreamHandler()
                    ])

# Define error constants
ERR_NON_DIGIT_UID = 'ERR001: Non-digit characters in UID'
ERR_RECORD_NOT_FOUND = 'ERR002: Record not found in Excel'
ERR_DUPLICATE_CONTROL_FIELD = 'ERR003: Duplicate Control Field'
ERR_NON_UTF8_ENCODING = 'ERR004: Non-UTF-8 encoding detected in field/subfield'
ERR_MISSING_UID = 'ERR005: Missing UID in MARC record'
ERR_EXCESSIVE_FIELD_REPETITIONS = 'ERR006: Excessive Field Repetitions'
ERR_CASE_SENSITIVITY_MISMATCH = 'ERR007: Case sensitivity mismatch'

error_entries = []

# Function to log errors
def diag_log(uid, title, error_code, error_message, additional_info=None):
    error_entry = {
        'UID/TN': uid,
        'Title': title,
        'Error Code': error_code,
        'Error Message': error_message
    }
    if additional_info:
        error_entry.update(additional_info)
    return error_entry

# Function to check for non-digit characters in UID
def check_non_digit_uid(uid):
    return not str(uid).isdigit()

# Function to check for duplicate control fields and presence of both '000' and 'LDR'
def check_duplicate_control_fields(record):
    control_fields = ['LDR', '000', '001', '003', '005', '006', '007', '008']
    field_counts = {field: 0 for field in control_fields}
    
    # Lambda function to check for both 'LDR' and '000'
    has_both_ldr_and_000 = lambda fields: any(field.tag == 'LDR' for field in fields) and any(field.tag == '000' for field in fields)
    
    for field in record.fields:
        if field.tag in control_fields:
            field_counts[field.tag] += 1
            if field_counts[field.tag] > 1:
                return True
    
    # Check if both '000' and 'LDR' fields are present
    if has_both_ldr_and_000(record.fields):
        return True
    
    return False

# Function to check for non-UTF-8 encoding in fields/subfields
def check_non_utf8_encoding(value):
    try:
        str(value).encode('utf-8')
    except UnicodeEncodeError:
        return True
    return False

# Function to compare strings with case sensitivity option
def compare_strings(str1, str2, case_sensitive=True):
    if str1 is None or str2 is None:
        return str1 == str2
    if case_sensitive:
        return str1 == str2
    else:
        return str1.lower() == str2.lower()

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
                    ERR_EXCESSIVE_FIELD_REPETITIONS,
                    f"Field {field_tag} repeated {count} times",
                    {'Field': field_tag, 'Repetitions': count}
                ))
    return repetitions_dict

# Main script
try:
    # Select MARC file
    mrc_file_path = select_file_to_open("*.mrc")

    # Load MARC records
    marc_records = load_marc_records(mrc_file_path)
    print(f"Loaded {len(marc_records)} MARC records.")
    logging.info("MARC records loaded successfully.")
    
    # Use max_repeats from config file (Placeholder value)
    max_repeats = config.MAX_REPEATS
    print(f"Maximum field repetitions allowed: {max_repeats}")

    # Analyze field repetitions and log errors
    repetitions_dict = analyze_field_repetitions(marc_records, max_repeats)
    print(f"Field repetition analysis completed. Found {len(repetitions_dict)} errors.")

    # Select folder to save the error logs
    folder_path = select_folder()
    print(f"Selected folder: {folder_path}")
    logging.info(f"Selected folder: {folder_path}")

    try:
        xlsx_file_path = select_file_to_open("*.xlsx")
        df = pd.read_excel(xlsx_file_path)
        print(f"Excel data loaded:\n{df.head(10)}")
        logging.debug(f"Excel data loaded:\n{df.head(10)}")
    except FileNotFoundError as e:
        print(f"Excel file not found: {e}")
        logging.error(f"Excel file not found: {e}")
    except pd.errors.EmptyDataError as e:
        print(f"Excel file is empty: {e}")
        logging.error(f"Excel file is empty: {e}")
    except pd.errors.ParserError as e:
        print(f"Error parsing Excel file: {e}")
        logging.error(f"Error parsing Excel file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while loading the Excel file: {e}")
        logging.error(f"An unexpected error occurred while loading the Excel file: {e}")

    # Debug: Print Excel data loaded
    print(f"Excel data loaded:\n{df.head(10)}")  # Print only the first few rows
    logging.debug(f"Excel data loaded:\n{df.head(10)}")  # Print only the first few rows

    # Check for records not found in Excel and delete matching dictionary entries
    for uid in list(repetitions_dict.keys()):
        if uid in df['001.1.'].values:
            del repetitions_dict[uid]
        else:
            repetitions_dict[uid].append(diag_log(uid, get_title(marc_records[uid]), ERR_RECORD_NOT_FOUND, "Record not found in Excel"))
    print(f"Records not found in tabular data removed; \nverified as correctly dropped. Remaining errors not found in tabular data: {len(error_entries)}")

    # Check for non-digit characters in UID and log errors
    for index, row in df.iterrows():
        uid = row['001.1.']
        if check_non_digit_uid(uid):
            error_entries.append(diag_log(uid, row['245$a'], ERR_NON_DIGIT_UID, "Non-digit characters in UID"))
    print(f"Non-digit UID check completed. Errors: {len(error_entries)}")

    # Check for duplicate control fields and log errors
    for uid, record in marc_records.items():
        if check_duplicate_control_fields(record):
            error_entries.append(diag_log(uid, get_title(record), ERR_DUPLICATE_CONTROL_FIELD, "Duplicate Control Field"))
    print(f"MaRC records duplicate control field check completed. Errors: {len(error_entries)}")

    # Check for non-UTF-8 encoding and log errors
    for index, row in df.iterrows():
        for col in row.index:
            value = row[col]
            if check_non_utf8_encoding(value):
                error_entries.append(diag_log(row['001.1.'], row['245$a'], ERR_NON_UTF8_ENCODING, "Non-UTF-8 encoding detected", {'Field': col}))
    print(f"Non-UTF-8 encoding check completed. Errors: {len(error_entries)}")

    # Compare strings with case sensitivity option
    fields_to_compare = {
        'LDR': 'LDR.1',
        '003': '003.1.',
        '008': '008.1.'
    }
    case_sensitive = True  # Set this to False if case-insensitive comparison is needed

    # Create hash map for dataframe
    df_map = {row['001.1.']: row for index, row in df.iterrows()}
    print("Hash map for dataframe created.")  # Debugging line

    # Iterate over the dictionary items (key-value pairs)
    for uid, record in marc_records.items():
        print(f"UID: {uid}")  # Debugging line
        if uid in df_map:
            df_row = df_map[uid]
            ### print(f"Dataframe row: {df_row}")  # Debugging line
            for marc_field, df_field in fields_to_compare.items():
                ### print(f"Checking MARC field: {marc_field}")  # Debugging line
                if marc_field in record:
                    marc_value = str(record[marc_field].value()) if record[marc_field] else ''
                else:
                    marc_value = ''
                df_value = str(df_row.get(df_field))
                if not compare_strings(marc_value, df_value, case_sensitive):
                    error_entries.append(diag_log(uid, df_row['LDR.1'], ERR_CASE_SENSITIVITY_MISMATCH, "Case sensitivity mismatch", {'Field': marc_field}))

    print(f"String comparison with case sensitivity check completed. Errors: {len(error_entries)}")

    # Debug: Print error entries after checking case sensitivity
    print(f"Error entries after checking case sensitivity: {error_entries[:10]}")  # Print only the first 10 entries
    logging.debug(f"Error entries after checking case sensitivity: {error_entries[:10]}")

    # Convert error entries to a Dask DataFrame
    if error_entries:
        error_log = dd.from_pandas(pd.DataFrame(error_entries), npartitions=4)
        base_name = os.path.splitext(os.path.basename(mrc_file_path))[0]

        # Get the unique error codes from the DataFrame
        unique_error_codes = error_log['Error Code'].unique().compute()
        print(f"Unique error codes: {unique_error_codes}")
        
        # Iterate over each unique error code and save the corresponding data to a TSV file
        for error_code in unique_error_codes:
            print(f"Processing error code: {error_code}")
            group = error_log[error_log['Error Code'] == error_code].compute()
            output_file_path = os.path.join(folder_path, f"{base_name}_error_log_{error_code}.tsv")
            
            # Log the file writing attempt
            print(f"Writing to {output_file_path}")
            logging.info(f"Writing to {output_file_path}")
            logging.debug(f"Data to write:\n{group.head()}")  # Print only the first few rows

            # Check if the group DataFrame is empty
            if group.empty:
                print(f"The group for error code {error_code} is empty.")
                logging.warning(f"The group for error code {error_code} is empty.")
            else:
                # Write the filtered DataFrame to a TSV file with tab separator
                group.to_csv(output_file_path, sep='\t', index=False)
                print(f"File {output_file_path} written successfully.")
                logging.info(f"File {output_file_path} written successfully.")

        # Check if the file is empty after writing
        if os.path.getsize(output_file_path) == 0:
            print(f"The file {output_file_path} is empty after writing.")
            logging.error(f"The file {output_file_path} is empty after writing.")
        else:
            print(f"File {output_file_path} written successfully.")
            logging.info(f"File {output_file_path} written successfully.")

        print(f"Field repetition analysis and error logging completed successfully. Logs saved to {folder_path}")
        logging.info(f"Field repetition analysis and error logging completed successfully. Logs saved to {folder_path}")
    else:
        print("No errors found. No logs to save.")
        logging.info("No errors found. No logs to save.")

except FileNotFoundError as e:
    print(f"File not found: {e}")
    logging.error(f"File not found: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    logging.error(f"An unexpected error occurred: {e}")