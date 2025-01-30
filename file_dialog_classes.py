import pandas as pd
import tkinter as tk
from tkinter import filedialog
import os

class FileDialog:
    def __init__(self, file_extensions):
        self.file_extensions = file_extensions
        self.selected_file_type = None

    def _format_file_extensions(self):
        return [(f"{ext} files", ext) for ext in self.file_extensions]

    def _get_file_type(self, file_path):
        _, ext = os.path.splitext(file_path)
        return ext

    def select_file_to_save(self, default_name=None):
        root = tk.Tk()
        root.withdraw()
        file_path = filedialog.asksaveasfilename(
            title="Select a file to save",
            filetypes=self._format_file_extensions(),
            initialfile=default_name
        )
        if not file_path:
            raise FileNotFoundError("No file selected.")
        self.selected_file_type = self._get_file_type(file_path)
        if not self.selected_file_type:
            # Append the default extension based on the selected file type
            self.selected_file_type = self.file_extensions[0]  # Default to the first extension if none is selected
            file_path += self.selected_file_type
        return file_path, self.selected_file_type

    def select_file_to_open(self):
        while True:
            root = tk.Tk()
            root.withdraw()
            file_path = filedialog.askopenfilename(
                title="Select a file to open",
                filetypes=self._format_file_extensions()
            )
            if file_path:
                self.selected_file_type = self._get_file_type(file_path)
                return file_path, self.selected_file_type
            else:
                user_input = input("No file selected. Do you want to retry? (y/n): ").strip().lower()
                if user_input != 'y':
                    print("Aborting the file selection.")
                    return None, None

    def create_file(self, file_path):
        with open(file_path, 'w') as file:
            file.write("")  # Create an empty file
        print(f"File created at: {file_path}")

class SaveFileDialog(FileDialog):
    def select_file_to_save(self, default_name=None):
        root = tk.Tk()
        root.withdraw()
        file_path = filedialog.asksaveasfilename(
            title="Select a file to save",
            filetypes=self._format_file_extensions(),
            initialfile=default_name
        )
        if not file_path:
            raise FileNotFoundError("No file selected.")
        
        # Store the selected file type in an attribute
        self.selected_file_type = self._get_file_type(file_path)

        # Ensure the file path has the correct extension
        if not file_path.endswith(self.selected_file_type):
            file_path += self.selected_file_type
        
        return file_path, self.selected_file_type

class OpenFileDialog(FileDialog):
    def select_file_to_open(self):
        return super().select_file_to_open()

class DataFrameSaver(FileDialog):
    def __init__(self, file_extensions, default_path=None):
        super().__init__(file_extensions)
        self.default_path = default_path

    def _log_error(self, message):
        with open("write_error_log.txt", "a") as log_file:
            log_file.write(message + "\n")

    def _handle_error(self, e, file_path):
        if isinstance(e, FileNotFoundError):
            error_message = f"File not found: {file_path}"
        elif isinstance(e, PermissionError):
            error_message = f"Permission denied: Unable to write to {file_path}"
        else:
            error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        self._log_error(error_message)

    def save_to_xlsx(self, df, file_path=None, chunk_size=1000):
        if file_path is None:
            file_path = self.default_path
        self.create_file(file_path)  # Ensure the file is created first
        try:
            standard_width = 17
            ind_width = 8

            writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
            workbook = writer.book
            workbook.strings_to_urls = False
            worksheet = workbook.add_worksheet()

            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value)
                if "IND" in value:
                    worksheet.set_column(col_num, col_num, ind_width)
                else:
                    worksheet.set_column(col_num, col_num, standard_width)

            row_num = 1

            if '001.1' in df.columns:
                field_001_index = df.columns.get_loc('001.1')
            else:
                field_001_index = None

            for start_row in range(0, len(df), chunk_size):
                end_row = min(start_row + chunk_size, len(df))
                chunk = df.iloc[start_row:end_row]
                for r in chunk.itertuples(index=False, name=None):
                    field_001 = r[field_001_index] if field_001_index is not None else 'N/A'
                    max_row_height = 1

                    for col_num, cell_value in enumerate(r):
                        cell_value_str = str(cell_value)
                        line_count = cell_value_str.count('\n') + 1
                        max_row_height = max(max_row_height, line_count)

                        if pd.isna(cell_value):
                            continue
                        worksheet.write(row_num, col_num, cell_value_str)
                    worksheet.set_row(row_num, max_row_height * 15)
                    row_num += 1
            writer.close()

        except Exception as e:
            self._handle_error(e, file_path)

    def save_to_parquet(self, df, file_path=None, **kwargs):
        if file_path is None:
            file_path = self.default_path
        self.create_file(file_path)  # Ensure the file is created first
        try:
            df.to_parquet(file_path, **kwargs)
            print(f"DataFrame saved to {file_path} as .parquet")
        except Exception as e:
            self._handle_error(e, file_path)