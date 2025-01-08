import os
import pandas as pd
import pymarc
import tkinter as tk
from tkinter import simpledialog
from file_handling import select_file, save_dropped_records, select_folder
from data_transformation import (
    extract_field_codes, filter_records, marc_to_dataframe, save_to_xlsxwriter_in_chunks
)
import config

# Creates a selection dialog for the user to input field codes to drop.
def create_selection_dialog(fields):
    root = tk.Tk()
    root.withdraw()  # Hide the root window

    # Categorize fields based on their first digit
    categories = {
        '0': [], '1': [], '2': [], '3': [], '4': [],
        '5': [], '6': [], '7': [], '8': [], '9': []
    }

    for field in fields:
        if field == 'LDR':
            categories['0'].append(field)
        else:
            first_digit = field[0]
            categories[first_digit].append(field)

    for section in categories:
        categories[section].sort()
        
    # Ensure 'LDR' is at the beginning of the '0' list
    if 'LDR' in categories['0']:
        categories['0'].remove('LDR')
        categories['0'].insert(0, 'LDR')

    # Create the prompt text
    prompt_text = "Enter the field codes to drop (comma-separated) from the following list:\n"
    for digit, field_list in categories.items():
        if field_list:
            prompt_text += f"{digit}: {', '.join(field_list)}\n"

    # Create a dialogue box for inputting field codes
    field_codes_input = simpledialog.askstring(
        title="Select Fields to Drop:",
        prompt=prompt_text
    )

    if field_codes_input:
        fields_to_drop = [field.strip() for field in field_codes_input.split(',')]
        print("Selected fields to drop:", fields_to_drop)
        return fields_to_drop
    else:
        print("No fields selected to drop.")
        return []

print("Starting the script...")

try:
    # Select folder
    folder_path = select_folder()
    print(f"Selected folder: {folder_path}")

    # Select MARC file
    marc_file_path = select_file()
    print(f"Selected MARC file: {marc_file_path}")

    df = pd.DataFrame()  # Replace with your actual DataFrame

    # Extract the field codes from the MARC file to populate the rules list
    print("Extracting field codes from the MARC file...")
    fields = extract_field_codes(marc_file_path)
    print(f"Extracted field codes: {fields}")

    # Call function to create selection dialog for fields to drop
    print("Creating selection dialog for fields to drop...")
    fields_to_drop = create_selection_dialog(fields)

    # Filter records based on repetition limit from config file
    max_repeats = config.MAX_REPEATS  # Use max_repeats from config file
    print(f"Filtering records with max repeats set to {max_repeats}...")
    filtered_records, dropped_records_001 = filter_records(marc_file_path, max_repeats)
    print(f"Filtered records count: {len(filtered_records)}")
    print(f"Dropped records count: {len(dropped_records_001)}")

    # Extract the base name of the MARC file (without extension)
    base_name = os.path.splitext(os.path.basename(marc_file_path))[0]

    # Save dropped records' 001 fields to a text file
    output_file = os.path.join(folder_path, f"{base_name}_dropped_records.txt")
    print(f"Saving dropped records' 001 fields to {output_file}...")
    save_dropped_records(dropped_records_001, output_file)

    # Convert filtered MARC records to DataFrame
    print("Converting filtered MARC records to DataFrame...")
    df = marc_to_dataframe(filtered_records, fields, fields_to_drop)
    print("Conversion to DataFrame completed.")

    # Save DataFrame to XLSX
    output_file = os.path.join(folder_path, f"{base_name}_output.xlsx")
    print(f"Saving DataFrame to {output_file}...")
    save_to_xlsxwriter_in_chunks(output_file, df)
    print(f"Data saved to {output_file}")

except pymarc.exceptions.ReaderError as e:
    print(f"ReaderError: Error reading MARC file - {e}")
except ValueError as e:
    print(f"ValueError: Issue with data conversion - {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

print("Script completed successfully.")