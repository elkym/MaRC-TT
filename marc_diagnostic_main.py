import dask.dataframe as dd
import pandas as pd
import os
import config
import warnings
from tqdm import tqdm
from datetime import datetime
import re
from marctt_classes import *  # Import all classes from marctt_classes.py
from marc_diag_utils import *  # Import all functions from marc_diag_utils.py
from file_handling import select_file_to_open, select_folder
from hashmap_utils import *  # Import all functions from hashmap_utils.py

global mrc_file_path, xlsx_file_path, df_map

error_entries = []

# Initialize comparators
static_comparator = StaticFieldComparator()
dynamic_comparator = DynamicFieldComparator()

# Custom warning handler
def warning_handler(message, category, filename, lineno, file=None, line=None):
    print(f"Warning: {message}")
    user_input = input("Data corruption detected. Do you want to abort the script? (y/n): ")
    if user_input.lower() == 'y':
        exit()
warnings.showwarning = warning_handler

try:
    # Select MARC file and load MARC records
    mrc_file_path = select_file_to_open("*.mrc")
    marc_records = load_marc_records(mrc_file_path)
    print(f"Loaded {len(marc_records)} MARC records.")
except FileNotFoundError as e:
    print(f"MARC file not found: {e}")
except Exception as e:
    print(f"An unexpected error occurred while loading MARC records: {e}")

try:
    # Use max_repeats from config file
    max_repeats = config.MAX_REPEATS
    print(f"Maximum field repetitions allowed: {max_repeats}")

    # Analyze field repetitions and log errors
    repetitions_dict = analyze_field_repetitions(marc_records, max_repeats)
    print(f"Field repetition analysis completed. Found {len(repetitions_dict)} errors.")
except Exception as e:
    print(f"An unexpected error occurred during field repetition analysis: {e}")

try:
    # Select folder to save the diagnostic logs
    folder_path = select_folder()
    print(f"Selected folder: {folder_path}")
except Exception as e:
    print(f"An unexpected error occurred while selecting folder: {e}")

try:
    # Load Excel data
    xlsx_file_path = select_file_to_open("*.xlsx")
    print(f"Preparing to load dataframe with Excel data from {xlsx_file_path}")
    df = pd.read_excel(xlsx_file_path, dtype=str)
    print(f"Excel data loaded:\n{df.head(10)}")
except FileNotFoundError as e:
    print(f"Excel file not found: {e}")
except pd.errors.EmptyDataError as e:
    print(f"Excel file is empty: {e}")
except pd.errors.ParserError as e:
    print(f"Error parsing Excel file: {e}")
except Exception as e:
    print(f"An unexpected error occurred while loading the Excel file: {e}")

timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

def hash_map_helper():
    perform_checks(df, df_map, repetitions_dict, error_entries, marc_records)
    save_hash_map(df_map, folder_path, timestamp, mrc_file_path, xlsx_file_path)
    save_metadata(mrc_file_path, xlsx_file_path, folder_path, timestamp)

try:
    df_map = None  # Initialize df_map

    # Check if the same files are selected
    if check_same_files(mrc_file_path, xlsx_file_path, folder_path):
        regenerate = input("The same files were selected as last time. Do you want to regenerate the hash map? (y/n): ")
        if regenerate.lower() == 'y':
            df_map = create_hash_map(df)
            hash_map_helper()  # First call
            print(f"Hash map created with {len(df_map)} entries.")
        else:
            df_map = load_hash_map(folder_path)
            if df_map is None:
                print("No existing hash map found. Regenerating...")
                df_map = create_hash_map(df)
                hash_map_helper()  # Second call
            elif not validate_and_sample_check(df_map, sample_fraction=0.2, early_stop=True):
                print("Loaded hash map is invalid. Regenerating...")
                df_map = create_hash_map(df)
                hash_map_helper()  # Third call
            else:
                print("Performing checks on loaded hash map...")
                perform_checks(df, df_map, repetitions_dict, error_entries, marc_records)
    else:
        df_map = create_hash_map(df)
        hash_map_helper()  # Fourth call
        print(f"Hash map created with {len(df_map)} entries.")

    # Initialize the IndicatorValidator
    indicator_validator = IndicatorValidator()

    # Validate indicators in MARC records
    try:
        invalid_indicators_marc = {}
        for uid, record in marc_records.items():
            # Exclude control fields
            filtered_fields = [field for field in record.fields if field.tag not in config.CONTROL_FIELDS]
            record.fields = filtered_fields
            
            invalid_indicators = indicator_validator.validate_marc_indicators(record)
            if invalid_indicators:
                invalid_indicators_marc[uid] = invalid_indicators
                for field_tag, indicators in invalid_indicators:
                    error_entries.append(diag_log(uid, get_title(record), config.ERR_INVALID_INDICATOR, f"Invalid indicators in field {field_tag}: {indicators}"))
    except UserWarning as e:
        print(e)

    # Use FIELDS_TO_COMPARE from config.py
    FIELDS_TO_COMPARE = config.FIELDS_TO_COMPARE
    print(f"Fields to compare loaded from config file: {FIELDS_TO_COMPARE}")

    # Validate indicators from tabular data using df_map
    try:
        invalid_indicators_tabular = indicator_validator.validate_tabular_indicators(df_map)
        for uid, col, indicator in invalid_indicators_tabular:
            print(str(df_map[uid]['245.1.a']))
            error_entries.append(diag_log(uid, df_map[uid]['245.1.a'], config.ERR_INVALID_INDICATOR, f"Invalid indicator in column {col}: {indicator}"))
    except UserWarning as e:
        print(e)
    try:
        print("Starting string comparisons...")
        for uid, record in tqdm(marc_records.items(), desc="Comparing records"):
            if uid in df_map:
                df_row = df_map[uid]
                for marc_field, field_info in FIELDS_TO_COMPARE.items():
                    try:
                        print(f"Comparing UID: {uid}, MARC Field: {marc_field}, Field Info: {field_info}")
                        marc_data = record.get_fields(marc_field)
                        tabular_data = df_row.get(marc_field, "")
                        print(f"MARC Data: {marc_data}, Type: {type(marc_data)}, Tabular Data: {tabular_data}, Type: {type(tabular_data)}")

                        if isinstance(field_info, list):
                            if not dynamic_comparator.compare_field(record, df_row, marc_field, field_info):
                                error_entries.append(diag_log(uid, df_row['LDR.1'], config.ERR_STRING_MISMATCH, "String mismatch", marc_data, tabular_data, {'Field': marc_field}))
                        else:
                            if not static_comparator.compare_field(record, df_row, marc_field, field_info):
                                error_entries.append(diag_log(uid, df_row['LDR.1'], config.ERR_STRING_MISMATCH, "String mismatch", marc_data, tabular_data, {'Field': marc_field}))
                    except Exception as e:
                        print(f"Error comparing UID: {uid}, MARC Field: {marc_field}, Field Info: {field_info}")
                        print(f"Exception: {e}")
                        print(f"MARC Data: {marc_data}, Type: {type(marc_data)}, Tabular Data: {tabular_data}, Type: {type(tabular_data)}")
                        error_entries.append(diag_log(uid, df_row['LDR.1'], config.ERR_STRING_MISMATCH, f"Exception during comparison: {e}", marc_data, tabular_data, {'Field': marc_field}))

        print(f"String comparison completed. Errors: {len(error_entries)}")
    except Exception as e:
        print(f"An unexpected error occurred during string comparisons: {e}")

    if error_entries:
        ## print(f"Error entries- {error_entries}")  # Debug statement
        error_log = dd.from_pandas(pd.DataFrame(error_entries), npartitions=4)
        ## print(f"Error log DataFrame:\n{error_log.compute()}")  # Debug statement
        base_name = os.path.splitext(os.path.basename(mrc_file_path))[0]
        unique_error_codes = error_log['Error Code'].unique().compute()
        print(f"Unique error codes: {unique_error_codes}")
        
        for error_code in tqdm(unique_error_codes, desc="Processing error codes"):
            print(f"Processing error code: {error_code}")
            group = error_log[error_log['Error Code'] == error_code].compute()
            ## print(f"Group DataFrame for error code {error_code}:\n{group}")  # Debug statement

            # Sanitize the error code to replace any non-alphanumeric characters with an underscore
            sanitized_error_code = re.sub(r'[^\w\s-]', '_', error_code).replace(' ', '_')
            output_file_path = os.path.join(folder_path, f"{base_name}_error_log_{sanitized_error_code}.tsv")
            print(f"Sanitized output file path: {output_file_path}")  # Debug statement

            ## print(f"DataFrame content before writing to {output_file_path}:\n{group}")  # Debug statement
            group.to_csv(output_file_path, sep='\t', index=False)

            # Verify file content after writing
            with open(output_file_path, 'r', encoding='utf-8') as f:
                file_content = f.read()
            ## print(f"File content after writing to {output_file_path}:\n{file_content}")  # Debug statement
            print(f"File {output_file_path} written successfully.")
                    
            # Check if the file is empty after writing
            if os.path.getsize(output_file_path) == 0:
                print(f"The file {output_file_path} is empty after writing.")
            else:
                print(f"File {output_file_path} written successfully.")
        print(f"Field repetition analysis and diagnostic logging completed successfully. Logs saved to {folder_path}")
    else:
        print("No errors found. No logs to save.")

except FileNotFoundError as e:
    print(f"File not found: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")