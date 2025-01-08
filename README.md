# MaRC-TT

## Description
MaRC-TT is a Python-based tool that uses PyMARC to flatten MARC (Machine-Readable Cataloging) data into a tabular dataset. It provides a user-friendly interface for selecting and dropping specific MARC fields, filtering records for unusual numbers of subfield repetitions . It then converts the data into a DataFrame which can be saved as an Excel file. Plans to create a reversal process to take tabular data and make it into a .mrc file are underway.

## Installation
### Prerequisites
- Python 3.x
- pandas
- pymarc
- tkinter

### Installation Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/elkym/MaRC-TT.git
   ```
2. Navigate to the project directory:
   ```bash
   cd MaRC-TT
   ```
3. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
Multiple scripts are available to run within this project. The primary script is `marc_to_tabular.py`. Other scripts will be added in future updates.

1. Run the primary script:
   ```bash
   python marc_to_tabular.py
   ```
2. Follow the prompts to select a MARC file and specify the fields to drop.
3. The script will filter the records, convert them to a DataFrame, and save the output as an Excel file.

## Features
- **Field Selection Dialog**: Allows users to select MARC fields to drop.
- **Record Filtering**: Filters records based on a specified repetition limit. (Repeatable subfields could make tabular data incredibly unwieldy, or impossible to even load in a spreadsheet or similar interface. Recommended settings for this variable are 20-30-- the variable is found in the config file.)
- **Data Conversion**: Converts MARC records to a pandas DataFrame.
- **Excel Export**: Saves the DataFrame to an Excel file. Future updates will allow export to parquet.
- **Error Reports**: Some errors are handled in the code, other kinds of errors can be checked with two tools: marc_diagnostic_main.py, and marc_field_repetition_report.py script.

## License
This project is licensed under the GNU License 3.0. See the LICENSE file for more information.

## Contact
For questions or support, please contact mykle.law@gmail.com
