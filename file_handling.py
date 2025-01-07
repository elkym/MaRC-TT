import pandas as pd
from tkinter import Tk, filedialog

def select_file(file_type, file_extension):
    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(
        title=f"Select {file_type} file",
        filetypes=[(file_type, file_extension)]
    )
    if not file_path:
        raise FileNotFoundError(f"No {file_type} file selected.")
    return file_path

def save_dropped_records(dropped_records_001, output_file):
    with open(output_file, 'w') as f:
        for record_001 in dropped_records_001:
            f.write(record_001 + '\n')