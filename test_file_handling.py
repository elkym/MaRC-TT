from file_dialog_classes import SaveFileDialog, OpenFileDialog, DataFrameSaver
from file_handling import select_folder
import pandas as pd
from datetime import datetime

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

# Create a sample DataFrame for testing
data = {
    'A': [1, 2, 3, 4],
    'B': [5, 6, 7, 8],
    'C': [9, 10, 11, 12]
}
df = pd.DataFrame(data)

# Define file extensions
file_extensions = ['.xlsx', '.parquet']

# Initialize DataFrameSaver with file extensions and default path
default_path = 'test_output.xlsx'
dataframe_saver = DataFrameSaver(file_extensions, default_path)

# Get the current date and time
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

# Prepopulate the save dialog with the base name and date-time
base_name = "example"
default_name = f"{base_name}_{current_datetime}"
save_dialog = SaveFileDialog([("Excel files", "*.xlsx"), ("Parquet files", "*.parquet")])
file_path, selected_file_type = save_dialog.select_file_to_save(default_name=default_name)

# Ensure the file path has the correct extension
if not file_path.endswith(selected_file_type):
    file_path += selected_file_type

print(f"Saving DataFrame to {file_path}...")

# Initialize DataFrameSaver and save DataFrame
exts_for_df = ["*.xlsx", "*.parquet"]
saver = DataFrameSaver(file_extensions=exts_for_df)
print("File path:", file_path)

if selected_file_type == '.xlsx':
    print("Saving as .xlsx")
    saver.save_to_xlsx(df, file_path)
elif selected_file_type == '.parquet':
    print("Saving as .parquet")
    saver.save_to_parquet(df, file_path)
else:
    print("Unknown file extension")

print(f"Data saved to {file_path}")