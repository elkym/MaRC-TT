import pandas as pd
import pymarc
import tkinter as tk
from tkinter import simpledialog
from file_handling import select_file, save_dropped_records
from data_transformation import (
    extract_field_codes, filter_records, marc_to_dataframe, save_to_xlsxwriter_in_chunks
)

def create_selection_dialog(fields):
    root = tk.Tk()
    root.withdraw()  # Hide the root window

    # Categorize fields based on their first digit
    categories = {
        '0': [],
        '1': [],
        '2': [],
        '3': [],
        '4': [],
        '5': [],
        '6': [],
        '7': [],
        '8': [],
        '9': []
    }

    for field in fields:
        if field == 'LDR':
            categories['0'].append(field)
        else:
            first_digit = field[0]
            categories[first_digit].append(field)

    # Create the prompt text
    prompt_text = "Enter the field codes to drop (comma-separated) from the following list:\n"
    for digit, field_list in categories.items():
        if field_list:
            prompt_text += f"{digit}: {', '.join(field_list)}\n"

    # Create a dialogue box for inputting field codes
    field_codes_input = simpledialog.askstring(
        title="Select Fields to Drop",
        prompt=prompt_text
    )

    if field_codes_input:
        fields_to_drop = [field.strip() for field in field_codes_input.split(',')]
        print("Selected fields to drop:", fields_to_drop)
        return fields_to_drop
    else:
        print("No fields selected to drop.")
        return []
# Example Usage:
print("Starting the script...")

marc_file_path = select_file("MARC", "*.mrc")  # Provide path to your MARC file
print(f"Selected MARC file: {marc_file_path}")

df = pd.DataFrame()  # Replace with your actual DataFrame

# Extract the field codes from the MARC file to populate the rules list
print("Extracting field codes from the MARC file...")
fields = extract_field_codes(marc_file_path)
print(f"Extracted field codes: {fields}")

# Call function to create selection dialog for fields to drop
print("Creating selection dialog for fields to drop...")
fields_to_drop = create_selection_dialog(fields)

# Filter records based on repetition limit
max_repeats = 15  # Set your desired maximum repeats
print(f"Filtering records with max repeats set to {max_repeats}...")
filtered_records, dropped_records_001 = filter_records(marc_file_path, max_repeats)
print(f"Filtered records count: {len(filtered_records)}")
print(f"Dropped records count: {len(dropped_records_001)}")

# Save dropped records' 001 fields to a text file
output_file = "dropped_records.txt"
print(f"Saving dropped records' 001 fields to {output_file}...")
save_dropped_records(dropped_records_001, output_file)

# Convert filtered MARC records to DataFrame
print("Converting filtered MARC records to DataFrame...")
df = marc_to_dataframe(filtered_records, fields, fields_to_drop)
print("Conversion to DataFrame completed.")

# Save DataFrame to XLSX
print("Saving DataFrame to XLSX...")
save_to_xlsxwriter_in_chunks(marc_file_path, df)
print(f"Data saved to {marc_file_path.replace('mrc', 'xlsx')}")

print("Script completed successfully.")