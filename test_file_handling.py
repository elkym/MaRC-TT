from file_handling import select_file, select_folder

def test_select_folder():
    try:
        folder_path = select_folder()
        print(f"Selected folder: {folder_path}")
    except FileNotFoundError as e:
        print(e)

def test_select_file():
    try:
        file_path = select_file()
        print(f"Selected file: {file_path}")
    except FileNotFoundError as e:
        print(e)

if __name__ == "__main__":
    print("Testing folder selection...")
    test_select_folder()
    
    print("\nTesting file selection...")
    test_select_file()