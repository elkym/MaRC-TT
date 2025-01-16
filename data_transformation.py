import pandas as pd
import pymarc
import dask.dataframe as dd
from general_utils import sort_key, sort_columns, generate_column_names # sort_key is a helper function called by sort_columns: see details in general_utils.py

def escape_line_breaks(data):
    return data.replace('\r', '\n')

def flatten_list(nested_list):
    flat_list = []
    for item in nested_list:
        if isinstance(item, list):
            flat_list.extend(flatten_list(item))  # Recursively flatten if item is a list
        else:
            flat_list.append(item)
    return flat_list

def extract_indicators(record, field_name, field_occurrence):
    """
    Extracts indicators from non-control MARC fields and returns them as a dictionary.

    Parameters:
    - record: The MARC record object.
    - field_name: The name of the MARC field.
    - field_occurrence: The occurrence count of the field.

    Returns:
    - A dictionary containing indicator data with column names as keys and indicator values as values.
    """
    indicator_data = {}
    control_fields = ['LDR', '001', '005', '006', '007', '008']
    if field_name not in control_fields:
        for field in record.get_fields(field_name):
            if field.indicators:
                indicator1 = field.indicators[0] if field.indicators[0].isdigit() else '#'
                indicator2 = field.indicators[1] if len(field.indicators) > 1 and field.indicators[1].isdigit() else '#'
                column_name = f"{field_name}.{field_occurrence}.IND"
                indicator_value = f"{indicator1}{indicator2}"
                indicator_data[column_name] = indicator_value
    return indicator_data

def marc_to_dataframe(filtered_records, rules, fields_to_drop):
    """
    Converts MARC records to a Dask DataFrame.

    Parameters:
    - filtered_records: A list of filtered MARC records.
    - rules: A dictionary containing rules for processing fields.
    - fields_to_drop: A list of fields to be dropped from the DataFrame.

    Returns:
    - A Dask DataFrame containing the transformed MARC records.
    """
    flattened_data = []
    try:
        for record in filtered_records:
            internal_record_count = {}
            row_data = {}
            row_data['LDR.1'] = str(record.leader)
            for field in record.fields:
                field_name = str(field.tag)
                if field_name not in rules or field_name in fields_to_drop:
                    continue
                if field_name not in internal_record_count:
                    internal_record_count[field_name] = 0
                internal_record_count[field_name] += 1
                field_occurrence = internal_record_count[field_name]
                subfield_counts = {}
                indicator_data = extract_indicators(record, field_name, field_occurrence)
                row_data.update(indicator_data)
                if field.subfields:
                    for subfield in field.subfields:
                        subfield_code = f"${subfield[0]}"
                        subfield_value = escape_line_breaks(subfield[1])
                        if subfield_code not in subfield_counts:
                            subfield_counts[subfield_code] = 0
                        subfield_counts[subfield_code] += 1
                        subfield_occurrence = subfield_counts[subfield_code]
                        column_name = generate_column_names(field_name, subfield_code, field_occurrence, subfield_occurrence)
                        row_data[column_name] = subfield_value
                else:
                    field_value = escape_line_breaks(field.data)
                    column_name = generate_column_names(field_name, '', field_occurrence, 1)
                    row_data[column_name] = field_value
            flattened_data.append(row_data)
        
        df = pd.DataFrame(flattened_data)
        sorted_columns = sort_columns(df.columns)
        df = df[sorted_columns]

        # Convert NaN values to empty strings unless they are 'N/A'
        df.fillna('', inplace=True)
        df.replace('nan', '', inplace=True)

        # Convert pandas DataFrame to Dask DataFrame
        ddf = dd.from_pandas(df, npartitions=10)

        # Define the replacement function
        def replace_line_breaks(df, columns):
            for col in columns:
                df[col] = df[col].str.replace('\n', '¦¦', regex=False)
            return df

        # List of columns to check for line breaks
        columns_to_check = [col for col in df.columns if col.startswith(('2', '5', '9'))]

        # Apply the replacement function in parallel
        ddf = ddf.map_partitions(replace_line_breaks, columns=columns_to_check)

        # Compute the result
        result_df = ddf.compute()

    except ValueError as e:
        print(f"ValueError: Issue with data conversion - {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    return result_df

def custom_write(worksheet, row, col, value, column_name, field_001, log_file):
    """
    Writes a value to an Excel worksheet cell and logs NaN values.
    This function was developed to help determine how various NaN values are being handled by the script-- as some fields in MaRC data are customized for specific systems:
    these may include values (particularly in the 9XX MaRC fields) that may be interpreted as NaN, yet need to be preserved instead of converted to empty strings.
    (The pandas function pd.isna() doesn't always act as expected.)

    Parameters:
    - worksheet: The Excel worksheet object.
    - row: The row number where the value should be written.
    - col: The column number where the value should be written.
    - value: The value to be written to the cell.
    - column_name: The name of the column (used for logging).
    - field_001: The 001 field value (used for logging).
    - log_file: The file object for logging NaN values.
    """
    if pd.isna(value):
        if value != '':
            log_file.write(f"NaN encountered at row {row}, col {col} (Column: {column_name}, 001 Field: {field_001}). Data type: {type(value)}, Data representation: {repr(value)}\n")
        worksheet.write(row, col, '')
    else:
        worksheet.write(row, col, value)

def save_to_xlsxwriter_in_chunks(output_file_path, df, chunk_size=1000):
    try:
        standard_width = 17
        ind_width = 8

        writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')
        workbook = writer.book
        workbook.strings_to_urls = False
        worksheet = workbook.add_worksheet()

        with open("xlsx_write_error_log.txt", "w") as log_file:
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

    except FileNotFoundError:
        print(f"File not found: {output_file_path}")
    except PermissionError:
        print(f"Permission denied: Unable to write to {output_file_path}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def load_marc_records(mrc_file_path):
    marc_records = {}
    with open(mrc_file_path, 'rb') as fh:
        reader = pymarc.MARCReader(fh)
        for record in reader:
            uid = record['001'].value()
            marc_records[uid] = record
    return marc_records

def get_title(record):
    if '245' in record and 'a' in record['245']:
        return record['245']['a']
    return 'Title not found'
