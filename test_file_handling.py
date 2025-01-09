from file_handling import select_file_to_open, select_file_to_save, select_folder
import config

def test_select_folder():
    try:
        folder_path = select_folder()
        print(f"Selected folder: {folder_path}")
    except FileNotFoundError as e:
        print(e)

def test_select_file_to_open():
    try:
        file_path = select_file_to_open("*.xlsx")
        print(f"Selected file to open: {file_path}")
    except FileNotFoundError as e:
        print(e)

def test_select_file_to_save():
    try:
        file_path = select_file_to_save("*.mrc", default_name="example.mrc")
        print(f"Selected file to save: {file_path}")
    except FileNotFoundError as e:
        print(e)

# Directly call the test functions
print("Testing folder selection...")
test_select_folder()

print("\nTesting file selection to open...")
test_select_file_to_open()

print("\nTesting file selection to save...")
test_select_file_to_save()