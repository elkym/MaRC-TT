import os
from tkinter import Tk, filedialog
import config
from datetime import datetime
import pymarc
from general_utils import has_excessive_repeats

def select_file_to_open(*file_extensions):
    """
    Opens a file dialog to select a file with the specified extensions for opening.

    Parameters:
    - file_extensions: Arbitrary number of file extensions to filter by (default is a predefined list).

    Returns:
    - The path to the selected file.
    """
    if not file_extensions:
        file_extensions = [
            ("MARC files", "*.mrc"),
            ("Excel files", "*.xlsx"),
            ("TSV files", "*.tsv"),
            ("Text files", "*.txt"),
            ("Parquet files", "*.parquet"),
            ("CSV files", "*.csv")
        ]
    else:
        file_extensions = [(f"{ext} files", ext) for ext in file_extensions]

    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(
        title="Select a file to open",
        filetypes=file_extensions
    )
    if not file_path:
        raise FileNotFoundError("No file selected.")
    return file_path

def select_file_to_save(*file_extensions, default_name=None):
    """
    Opens a file dialog to select a file with the specified extensions for saving.

    Parameters:
    - file_extensions: Arbitrary number of file extensions to filter by (default is a predefined list).
    - default_name: Optional default file name for the save dialog.

    Returns:
    - The path to the selected file.
    """
    if not file_extensions:
        file_extensions = [
            ("MARC files", "*.mrc"),
            ("Excel files", "*.xlsx"),
            ("TSV files", "*.tsv"),
            ("Text files", "*.txt"),
            ("Parquet files", "*.parquet"),
            ("CSV files", "*.csv")
        ]
    else:
        file_extensions = [(f"{ext} files", ext) for ext in file_extensions]

    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.asksaveasfilename(
        title="Select a file to save",
        filetypes=file_extensions,
        initialfile=default_name
    )
    if not file_path:
        raise FileNotFoundError("No file selected.")
    return file_path

def select_folder():
    if config.USE_DEFAULT_FOLDER:
        return config.DEFAULT_FOLDER_PATH

    root = Tk()
    root.withdraw()  # Hide the root window
    folder_path = filedialog.askdirectory(title="Select a folder")
    if not folder_path:
        if not config.DEFAULT_FOLDER_PATH:
            raise FileNotFoundError("No folder selected and no default folder path set.")
        folder_path = config.DEFAULT_FOLDER_PATH
    return folder_path

def save_dropped_records(dropped_records_001, output_file):
    with open(output_file, 'w') as f:
        for record_001 in dropped_records_001:
            f.write(record_001 + '\n')

def get_last_modified_time(file_path):
    timestamp = os.path.getmtime(file_path)
    last_modified_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return last_modified_time

def extract_field_codes(marc_file_path):
    """
    Extracts field codes from a MARC file.

    Parameters:
    - marc_file_path: The path to the MARC file.

    Returns:
    - A list of field codes.
    """
    field_codes = set()
    with open(marc_file_path, 'rb') as fh:
        records = pymarc.MARCReader(fh)
        for record in records:
            field_codes.add('LDR')
            for field in record.fields:
                field_codes.add(str(field.tag))
    return list(field_codes)

def filter_records(marc_file_path, max_repeats):
    """
    Filters records based on a maximum repetition limit.

    Parameters:
    - marc_file_path: The path to the MARC file.
    - max_repeats: The maximum number of repetitions allowed for a field.

    Returns:
    - A tuple containing the filtered records and the dropped records' 001 fields.
    """
    filtered_records = []
    dropped_records_001 = []
    try:
        with open(marc_file_path, 'rb') as fh:
            records = pymarc.MARCReader(fh)
            for record in records:
                drop_record = False
                for field in record.fields:
                    if has_excessive_repeats(record, field.tag, max_repeats):
                        drop_record = True
                        break
                if drop_record:
                    field_001 = record['001'].value() if record['001'] else 'No 001 Field'
                    dropped_records_001.append(field_001)
                else:
                    filtered_records.append(record)
    except FileNotFoundError:
        print(f"File not found: {marc_file_path}")
    except pymarc.exceptions.FatalReaderError as e:
        print(f"Error reading MARC file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return filtered_records, dropped_records_001