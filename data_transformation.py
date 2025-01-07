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
    if subfield_count > 1:
        return f"{field_name}.{internal_count}.{subfield_code.strip('$')}.{subfield_count}"
    else:
        return f"{field_name}.{internal_count}.{subfield_code.strip('$')}"

# Function to check for excessive repeats of a field
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
    return filtered_records, dropped_records_001

def marc_to_dataframe(filtered_records, rules, fields_to_drop):
    flattened_data = []
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
    return df

def custom_write(worksheet, row, col, value, column_name, field_001, log_file):
    if pd.isna(value):
        if value != '':
            log_file.write(f"NaN encountered at row {row}, col {col} (Column: {column_name}, 001 Field: {field_001}). Data type: {type(value)}, Data representation: {repr(value)}\n")
        worksheet.write(row, col, '')
    else:
        worksheet.write(row, col, value)

def save_to_xlsxwriter_in_chunks(marc_file_path, df, chunk_size=1000):
    """
    Saves the DataFrame to an XLSX file in chunks, using the original MARC file name with .xlsx extension.
    
    Parameters:
    - marc_file_path: Path to the original MARC file.
    - df: The DataFrame containing the data to be saved.
    - chunk_size: The number of rows to write in each chunk (default is 1000).
    """
    # Define default widths
    standard_width = 17
    ind_width = 6.5

    # Generate the output file path by replacing the .mrc extension with .xlsx
    output_file_path = marc_file_path.replace(".mrc", ".xlsx")
    # Create an Excel writer object using xlsxwriter as the engine
    writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')
    # Get the workbook and add a worksheet
    workbook = writer.book
    # workbook.use_zip64()
    workbook.strings_to_urls = False
    worksheet = workbook.add_worksheet()

    # Open a log file for writing error messages
    with open("error_log.txt", "w") as log_file:
        # Write the header row with column names
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value)
            if "IND" in value:
                worksheet.set_column(col_num, col_num, ind_width)
            else:
                worksheet.set_column(col_num, col_num, standard_width)

        # Initialize the row number for writing data
        row_num = 1

        # Define the wrap format with text wrapping enabled
        wrap_format = workbook.add_format({'text_wrap': True})

        # Check if the '001.1' column exists
        if '001.1' in df.columns:
            field_001_index = df.columns.get_loc('001.1')
        else:
            field_001_index = None

        # Write data in chunks to avoid memory issues
        for start_row in range(0, len(df), chunk_size):
            end_row = min(start_row + chunk_size, len(df))
            chunk = df.iloc[start_row:end_row]
            for r in chunk.itertuples(index=False, name=None):
                # Extract the correct 001 field value if the column exists
                field_001 = r[field_001_index] if field_001_index is not None else 'N/A'
                max_row_height = 1  # Initialize max row height to 1 line

                for col_num, cell_value in enumerate(r):
                    cell_value_str = str(cell_value)

                    # Count number of lines based on newline characters and set row height accordingly
                    line_count = cell_value_str.count('\n') + 1
                    max_row_height = max(max_row_height, line_count)

                    # Check if the cell value is NaN and skip writing if it is
                    if pd.isna(cell_value):
                        continue

                    worksheet.write(row_num, col_num, cell_value_str, wrap_format)

                    # Use the custom_write function to handle NaN values and log errors
                    custom_write(worksheet, row_num, col_num, cell_value_str, df.columns[col_num], field_001, log_file)

                # Set row height based on max_row_height determined by newline characters
                worksheet.set_row(row_num, max_row_height * 15)  # Assuming default line height is 15 points

                row_num += 1

    # Close the writer to save the file
    writer.close()