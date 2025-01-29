from file_dialog_classes import SaveFileDialog, OpenFileDialog, FileDialog, DataFrameSaver
from file_handling import select_folder
import pandas as pd

def test_select_folder():
    try:
        folder_path = select_folder()
        print(f"Selected folder: {folder_path}")
    except FileNotFoundError as e:
        print(e)

def test_select_file_to_open():
    try:
        open_dialog = OpenFileDialog([("Excel files", "*.xlsx")])
        file_path, file_type = open_dialog.select_file_to_open()
        print(f"Selected file to open: {file_path}")
        print(f"Selected file type: {file_type}")
    except FileNotFoundError as e:
        print(e)

def test_select_file_to_save():
    try:
        save_dialog = SaveFileDialog([("MARC files", "*.mrc")])
        file_path, file_type = save_dialog.select_file_to_save(default_name="example.mrc")
        print(f"Selected file to save: {file_path}")
        print(f"Selected file type: {file_type}")
    except FileNotFoundError as e:
        print(e)

def test_open_csv_file():
    try:
        open_dialog = OpenFileDialog([("CSV files", "*.csv")])
        file_path, file_type = open_dialog.select_file_to_open()
        print(f"Selected CSV file to open: {file_path}")
        print(f"Selected file type: {file_type}")
    except FileNotFoundError as e:
        print(e)

def test_save_excel_file():
    try:
        save_dialog = SaveFileDialog([("Excel files", "*.xlsx")])
        file_path, file_type = save_dialog.select_file_to_save(default_name="example.xlsx")
        print(f"Selected Excel file to save: {file_path}")
        print(f"Selected file type: {file_type}")
    except FileNotFoundError as e:
        print(e)

def test_save_dataframe_to_parquet():
    try:
        df = pd.DataFrame({
            'A': [1, 2, 3],
            'B': [4, 5, 6],
            'C': [7, 8, 9]
        })
        saver = DataFrameSaver([("Parquet files", "*.parquet")])
        file_path, file_type = saver.select_file_to_save(default_name="example.parquet")
        saver.save_to_parquet(df, file_path)
        print(f"DataFrame saved to Parquet file: {file_path}")
    except FileNotFoundError as e:
        print(e)

# Directly call the test functions
print("Testing folder selection...")
test_select_folder()

print("\nTesting file selection to open...")
test_select_file_to_open()

print("\nTesting file selection to save...")
test_select_file_to_save()

print("\nTesting opening a CSV file...")
test_open_csv_file()

print("\nTesting saving an Excel file...")
test_save_excel_file()

print("\nTesting saving a DataFrame to Parquet...")
test_save_dataframe_to_parquet()