from file_handling import select_file

def test_select_file():
    try:
        file_path = select_file()
        print(f"Selected file: {file_path}")
    except FileNotFoundError as e:
        print(e)

if __name__ == "__main__":
    test_select_file()