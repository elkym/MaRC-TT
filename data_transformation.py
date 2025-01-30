import pandas as pd
import dask.dataframe as dd
from general_utils import sort_key, sort_columns, generate_column_names # sort_key is a helper function called by sort_columns: see details in general_utils.py
import config

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
    '''
    Extracts indicators from non-control MARC fields and returns them as a dictionary.

    Parameters:
    - record: The MARC record object.
    - field_name: The name of the MARC field.
    - field_occurrence: The occurrence count of the field.

    Returns:
    - A dictionary containing indicator data with column names as keys and indicator values as values.
    '''
    indicator_data = {}
    if field_name not in config.CONTROL_FIELDS:
        for field in record.get_fields(field_name):
            if field.indicators:
                indicator1 = field.indicators[0] if field.indicators[0].isdigit() else '#'
                indicator2 = field.indicators[1] if len(field.indicators) > 1 and field.indicators[1].isdigit() else '#'
                column_name = f"{field_name}.{field_occurrence}.IND"
                indicator_value = f"{indicator1}{indicator2}"
                indicator_data[column_name] = indicator_value
    return indicator_data

def marc_to_dataframe(filtered_records, rules, fields_to_drop):
    '''
    Converts MARC records to a Dask DataFrame.

    Parameters:
    - filtered_records: A list of filtered MARC records.
    - rules: A dictionary containing rules for processing fields.
    - fields_to_drop: A list of fields to be dropped from the DataFrame.

    Returns:
    - A Dask DataFrame containing the transformed MARC records.
    '''
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
