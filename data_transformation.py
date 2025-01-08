import os
import pandas as pd
import pymarc

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

def generate_column_names(field_name, subfield_code, internal_count, subfield_count):
    """
    Generates column names for the DataFrame based on MARC field and subfield information.
    The column names indicate the repetition of fields and subfields.
    Example: 041.1.a.1, 041.1.a.2
    In this example, these are both from the first instance of field 041, but indicate a repetition of subfield a, instance 1 and 2 of subfield a are denoted.
    
    Example: 600.1.IND, 600.1.a, 600.1.b, 600.2.IND, 600.2.a, 600.2.b
    In this example, the 600 field as a whole has been repeated.
    These columns indicate the indicators and subfield content for subfields a and b for the first and second instance of field 600.

    Parameters:
    - field_name: The name of the MARC field.
    - subfield_code: The code of the MARC subfield.
    - internal_count: The internal count of the field occurrence.
    - subfield_count: The count of the subfield occurrence.

    Returns:
    - A string representing the generated column name.
    """
    if subfield_count > 1:
        return f"{field_name}.{internal_count}.{subfield_code.strip('$')}.{subfield_count}"
    else:
        return f"{field_name}.{internal_count}.{subfield_code.strip('$')}"

def has_excessive_repeats(record, field_tag, max_repeats):
    count = sum(1 for field in record.get_fields(field_tag))
    return count > max_repeats

def extract_field_codes(marc_file_path):
    field_codes = set()
    with open(marc_file_path, 'rb') as fh:
        records = pymarc.MARCReader(fh)
        for record in records:
            field_codes.add('LDR')
            for field in record.fields:
                field_codes.add(str(field.tag))
    return list(field_codes)

def sort_columns(columns):
    def sort_key(column_name):
        parts = column_name.split('.')
        if parts[0] == 'LDR':
            field_tag = -1
            field_occurrence = 1
            subfield_code = ''
            subfield_occurrence = 0
        else:
            field_tag = int(parts[0])
            field_occurrence = int(parts[1])
            subfield_code = parts[2] if len(parts) > 2 else ''
            subfield_occurrence = int(parts[3]) if len(parts) > 3 else 0
        return (field_tag, field_occurrence, subfield_code, subfield_occurrence)
    return sorted(columns, key=sort_key)

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

def filter_records(marc_file_path, max_repeats):
    filtered_records = []
    dropped_records_001 = []
    try:
        with open(marc_file_path, 'rb') as fh:
            records = pymarc.MARCReader(fh)
            for record in records:
                drop_record = False
                for field in record.fields:
                    if has_excessive_repeats(record, field.tag, max_repeats):
                        drop_record = True
                        break
                if drop_record:
                    field_001 = record['001'].value() if record['001'] else 'No 001 Field'
                    dropped_records_001.append(field_001)
                else:
                    filtered_records.append(record)
    except FileNotFoundError:
        print(f"File not found: {marc_file_path}")
    except pymarc.exceptions.ReaderError as e:
        print(f"Error reading MARC file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return filtered_records, dropped_records_001

def marc_to_dataframe(filtered_records, rules, fields_to_drop):
    """
    Converts MARC records to a pandas DataFrame.

    Parameters:
    - filtered_records: A list of filtered MARC records.
    - rules: A dictionary containing rules for processing fields.
    - fields_to_drop: A list of fields to be dropped from the DataFrame.

    Returns:
    - A pandas DataFrame containing the transformed MARC records.
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

        df = df.map(str)

    except ValueError as e:
        print(f"ValueError: Issue with data conversion - {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    return df

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
        ind_width = 6.5

        writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')
        workbook = writer.book
        workbook.strings_to_urls = False
        worksheet = workbook.add_worksheet()

        with open("error_log.txt", "w") as log_file:
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value)
                if "IND" in value:
                    worksheet.set_column(col_num, col_num, ind_width)
                else:
                    worksheet.set_column(col_num, col_num, standard_width)

            row_num = 1
            wrap_format = workbook.add_format({'text_wrap': True})

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

                        worksheet.write(row_num, col_num, cell_value_str, wrap_format)
                        custom_write(worksheet, row_num, col_num, cell_value_str, df.columns[col_num], field_001, log_file)

                    worksheet.set_row(row_num, max_row_height * 15)

                    row_num += 1

        writer.close()
    except FileNotFoundError:
        print(f"File not found: {output_file_path}")
    except PermissionError:
        print(f"Permission denied: Unable to write to {output_file_path}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")