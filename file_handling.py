import os
from tkinter import Tk, filedialog

def select_file(*file_extensions):
    """
    Opens a file dialog to select a file with the specified extensions.

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
            ("All files", "*.*")
        ]
    else:
        file_extensions = [(f"{ext} files", ext) for ext in file_extensions]

    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(
        title="Select a file",
        filetypes=file_extensions
    )
    if not file_path:
        raise FileNotFoundError("No file selected.")
    return file_path

def select_folder():
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