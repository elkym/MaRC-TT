import pandas as pd
from tkinter import Tk, filedialog

def select_file():
    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(
        title="Select a file",
        filetypes=[
            ("MARC files", "*.mrc"),
            ("Excel files", "*.xlsx"),
            ("TSV files", "*.tsv"),
            ("Text files", "*.txt"),
            ("Parquet files", "*.parquet")
        ]
    )
    if not file_path:
        raise FileNotFoundError("No file selected.")
    return file_path

def save_dropped_records(dropped_records_001, output_file):
    with open(output_file, 'w') as f:
        for record_001 in dropped_records_001:
            f.write(record_001 + '\n')