import logging
import os
from datetime import datetime
import pandas as pd
from pymarc import Record, Field
from file_handling import select_file_to_open, select_file_to_save

# Set up logging
logging.basicConfig(filename='NA_error_log.txt', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

origin_file = select_file_to_open("*.xlsx")
if not origin_file:
    print("No file selected for origin. Aborting script.")
    exit()
print("Origin file selected:", origin_file)

# Generate default destination file name
base_name = os.path.splitext(os.path.basename(origin_file))[0]
current_date = datetime.now().strftime("%m%d")
default_destination_name = f"{base_name}_{current_date}.mrc"

destination_file = select_file_to_save("*.mrc", default_name=default_destination_name)
if not destination_file:
    print("No file selected for destination. Aborting script.")
    exit()
print("Destination file selected:", destination_file)

# Try-except block to handle potential exceptions when reading the Excel file
try:
    # Read data from Excel with all columns specified as strings and 'N/A' treated as a string
    df = pd.read_excel(origin_file, dtype=str, na_values=['', 'NaN', 'null'])
    # Replace NaN values with 'N/A' in the specific column
    df['952.1.c'].fillna('N/A', inplace=True)
    # Debugging print statement
    if '952.1.c' in df.columns:
        print(f"Data type of '952.1.c': {df['952.1.c'].dtype}")
        print(f"Data in '952.1.c':\n{df['952.1.c'].head()}")
except FileNotFoundError:
    print(f"Error: The file {origin_file} was not found. Please select a valid file.")
    exit()
except pd.errors.EmptyDataError:
    print(f"Error: The file {origin_file} is empty. Please select a different file.")
    exit()
except pd.errors.ParserError:
    print(f"Error: There was a parsing error while reading the file {origin_file}. Please select a different file.")
    exit()
except ValueError as e:
    print(f"Error: {e}. Please check the file format and data types.")
    exit()
except IOError as e:
    print(f"Error: An I/O error occurred while reading the file {origin_file}: {e}. Please check file permissions and try again.")
    exit()

# Global variable for invalid records, initialized as an empty DataFrame
invalid_records_df = pd.DataFrame()

# Function to parse header and extract field, occurrence, subfield, and subfield occurrence number
def parse_header(header):
    """
    Parse the header to extract field, occurrence, subfield, and subfield occurrence number.
    
    Args:
        header (str): The header string from the Excel file.
        Returns:
        tuple: A tuple containing field, occurrence, subfield, and subfield occurrence number.
    """
    parts = header.split('.')
    field = parts[0]
    occurrence = parts[1] if len(parts) > 1 else None
    subfield = parts[2] if len(parts) > 2 else None
    subfield_occurrence = parts[3] if len(parts) > 3 else None
    return field, occurrence, subfield, subfield_occurrence

# Function to validate header format
def validate_header(header, control_fields):
    parts = header.split('.')
    
    if len(parts) < 1 or len(parts) > 4:
        return False, "Header has an incorrect number of parts"
    
    # Validate field part
    field = parts[0]
    if field not in ['000', 'LDR'] and (not field.isdigit() or len(field) != 3):
        return False, "Field part is invalid"
    
    # If the field is a control field, it should not have more than one part
    if field in control_fields:
        if len(parts) > 1:
            return True, f"Header contains empty parts, {header}"
        return True, ""
    
    # Validate occurrence part
    if len(parts) > 1:
        occurrence = parts[1]
        if not occurrence.isdigit():
            return False, "Occurrence part is invalid"
    
    # Validate subfield part
    if len(parts) > 2:
        subfield = parts[2].lower()
        if subfield != 'ind' and not (subfield.isalnum() and len(subfield) == 1):
            return False, "Subfield part is invalid"
    
    # Validate subfield occurrence part
    if len(parts) > 3:
        subfield_occurrence = parts[3]
        if not subfield_occurrence.isdigit():
            return False, "Subfield occurrence part is invalid"
    
    # Check for empty parts
    if '' in parts:
        return True, f"Header contains empty parts, {header}"
    
    return True, ""

# List of control fields
control_fields = ['LDR', '001', '003', '005', '006', '007', '008']

# Validate all headers before processing
for column_name in df.columns:
    is_valid, error_message = validate_header(column_name, control_fields)
    if not is_valid:
        print(f"Error in column '{column_name}': {error_message}")
        print("Aborting script due to invalid headers.")
        exit()
print("All headers validated successfully.")

# Function to validate and clean indicator data
def clean_indicator_data(indicator_data, row, column_name):
    """
    Validate and clean the indicator data.
    
    Args:
        indicator_data (str): The raw indicator data.
        row (pd.Series): The current row being processed.
        column_name (str): The name of the column with the indicator data.
    
    Returns:
        str: The cleaned indicator data.
    """
    global invalid_records_df
    
    if pd.isna(indicator_data):
        return '\\\\'
    
    # Replace spaces with backslashes and clean the data
    cleaned_data = str(indicator_data).replace('#', '\\').replace(' ', '\\').strip()
    
    # Ensure the cleaned data is exactly two characters long and consists of digits or backslashes
    if len(cleaned_data) > 2:
        logging.error(f"Record/TN has invalid indicators in field {column_name}. Record has been dropped to invalid_indicators_records.xlsx.")
        row['Error'] = f"Invalid indicators (3+ characters) in field {column_name}"
        invalid_records_df = invalid_records_df.append(row, ignore_index=True)
        return None
    elif len(cleaned_data) < 2:
        logging.error(f"Record/TN has single-digit indicators in field {column_name}. Record has been dropped to invalid_indicators_records.xlsx.")
        row['Error'] = f"Single-digit indicators in field {column_name}"
        invalid_records_df = invalid_records_df.append(row, ignore_index=True)
        return None
    elif not all(c.isdigit() or c == '\\' for c in cleaned_data):
        logging.error(f"Record/TN has invalid characters in indicators in field {column_name}. Record has been dropped to invalid_indicators_records.xlsx.")
        row['Error'] = f"Invalid characters in indicators in field {column_name}"
        invalid_records_df = invalid_records_df.append(row, ignore_index=True)
        return None
    
    return cleaned_data

# Function to handle control fields
def handle_control_fields(fields, field, data):
    """
    Handle control fields (e.g., 001, 005).
    
    Args:
        fields (dict): The dictionary of fields.
        field (str): The field code.
        data (str): The data for the control field.
    """
    if not data:
        logging.error(f"Error: Missing or invalid data for control field {field}.")
        return
    
    if field not in fields:
        fields[field] = {'field_obj': Field(tag=field, data=data), 'subfield_data_present': True}

# Function to handle variable fields
def handle_variable_fields(fields, field, occurrence, subfield, subfield_occurrence, data, row, column_name):
    """
    Handle variable fields and set indicators or add subfields as needed.
    
    Args:
        fields (dict): The dictionary of fields.
        field (str): The field code.
        occurrence (str): The occurrence number.
        subfield (str): The subfield code.
        subfield_occurrence (str): The subfield occurrence number.
        data (str): The data for the variable field.
        row (pd.Series): The current row being processed.
        column_name (str): The name of the column with the indicator data.
    """
    key = f"{field}.{occurrence}"
    
    if key not in fields:
        fields[key] = {'field_obj': Field(tag=field, indicators=[' ', ' '], subfields=[]), 'subfield_data_present': False}
    
    if subfield == 'IND':
        indicators = clean_indicator_data(data, row, column_name)
        if indicators is None:
            return
        fields[key]['field_obj'] = Field(
            tag=fields[key]['field_obj'].tag,
            indicators=[indicators[0], indicators[1]],
            subfields=fields[key]['field_obj'].subfields
        )
    
    elif subfield:
        if not pd.isna(data) or data == 'N/A':
            # Debugging print statement
            if field == '952' and subfield == 'c':
                print(f"Original data type of '952.c': {type(data)}")
                print(f"Original data in '952.c': {data}")
            data = str(data)
            record_id = row['001'] if '001' in row else 'Unknown'
            logging.debug(f"Adding subfield: {subfield} with data: '{data}' for record ID: {record_id} in field: {field}")
            fields[key]['field_obj'].add_subfield(subfield, data)
            fields[key]['subfield_data_present'] = True

# Function to process each row of the Excel file
def process_row(row):
    """
    Process each row of the Excel file to create a MARC record.
    
    Args:
        row (pd.Series): A row from the DataFrame.
    
    Returns:
        Record: A MARC record created from the row data.
    """
    record = Record()
    fields_dict = {}
    
    # Iterate over each column header in the DataFrame
    for header in df.columns:
        field, occurrence, subfield, subfield_occurrence = parse_header(header)
        
        # Handle control fields separately
        if field in ['001', '005', '006', '007', '008']:
            handle_control_fields(fields_dict, field, row[header])
        else:
            # Ignore empty parts for control fields
            if subfield == '':
                continue
            current_subfield_data = row[header]
            if field == '952' and subfield == 'c':
                print(f"Processing '952.c': {current_subfield_data}")
            handle_variable_fields(fields_dict, field, occurrence, subfield, subfield_occurrence, current_subfield_data, row, header)
    
    # Add all processed fields to the MARC record
    for field_info in fields_dict.values():
        if field_info['subfield_data_present']:
            record.add_field(field_info['field_obj'])
    
    return record

# Create a list to hold MARC records by processing each row of the DataFrame
records = [process_row(row) for index, row in df.iterrows()]
print("All rows processed into MARC records.")

# Function to write MARC records to a file
def write_records_to_file(records, destination_file):
    """
    Write MARC records to a file.
    
    Args:
        records (list): A list of MARC records.
        destination_file (str): The path to the destination file.
    """
    try:
        with open(destination_file, 'wb') as file:
            for record in records:
                for field in record.get_fields('952'):
                    if 'c' in field:
                        print(f"Writing subfield '952.c': {field['c']}")
                file.write(record.as_marc())
        print("MARC records have been written to", destination_file)
    except IOError as e:
        print(f"Error writing to file {destination_file}: {e}")

# Write the MARC records to a .mrc file
write_records_to_file(records, destination_file)
print("MARC records have been written to", destination_file)

# At the end of the script
if not invalid_records_df.empty:
    base_name = os.path.splitext(os.path.basename(origin_file))[0]
    current_date = datetime.now().strftime("%Y%m%d")
    # Insert headers from the original DataFrame
    invalid_records_df.columns = df.columns
    invalid_records_df.to_excel(f'{base_name}_invalid_indicators_{current_date}.xlsx', index=False)
    print(f"Invalid records saved to '{base_name}_invalid_indicators_{current_date}.xlsx'.")