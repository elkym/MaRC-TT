import os
import pandas as pd
import pymarc
import tkinter as tk
from tkinter import simpledialog
from file_dialog_classes import OpenFileDialog, SaveFileDialog, DataFrameSaver
from data_transformation import marc_to_dataframe
from file_handling import select_folder, extract_field_codes, filter_records, save_dropped_records
import config
from datetime import datetime

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

    for section, items in categories.items():
        items.sort()
        
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
    # Initialize file dialogs
    open_dialog = OpenFileDialog(["*.mrc"])
    save_dialog = SaveFileDialog(["*.xlsx", "*.parquet"])

    # Select MARC file
    marc_file_path, _ = open_dialog.select_file_to_open()
    print(f"Selected MARC file: {marc_file_path}")

    # Select folder
    folder_path = select_folder()
    print(f"Selected folder: {folder_path}")

    # Extract the field codes from the MARC file to populate the rules list
    print("Extracting field codes from the MARC file...")
    fields = extract_field_codes(marc_file_path)
    print(f"Extracted field codes: {fields.sort()}")

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

    df = pd.DataFrame()  # Overwrite later w/DataFrame

    # Convert filtered MARC records to DataFrame
    print("Converting filtered MARC records to DataFrame...")
    df = marc_to_dataframe(filtered_records, fields, fields_to_drop)
    print("Conversion to DataFrame completed.")

    # Get the current date and time
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Prepopulate the save dialog with the base name and date-time
    default_name = f"{base_name}_{current_datetime}"
    file_path, _ = save_dialog.select_file_to_save(default_name=default_name)
    print(f"Saving DataFrame to {file_path}...")

    # Initialize DataFrameSaver and save DataFrame
    exts_for_df = ["*.xlsx", "*.parquet"]
    saver = DataFrameSaver(file_extensions=exts_for_df)
    print("File path:", file_path)

    # Ensure the file is created first
    saver.create_file(file_path)

    if file_path.endswith('.xlsx'):
        print("Saving as .xlsx")
        saver.save_to_xlsx(df, file_path)
    elif file_path.endswith('.parquet'):
        print("Saving as .parquet")
        saver.save_to_parquet(df, file_path)
    else:
        print("Unknown file extension")

    print(f"Data saved to {file_path}")

except pymarc.exceptions.FatalReaderError as e:
    print(f"ReaderError: Error reading MARC file - {e}")
except ValueError as e:
    print(f"ValueError: Issue with data conversion - {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

print("Script completed successfully.")