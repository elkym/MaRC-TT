def sort_key(column_name):
    """
    Generates a sorting key for column names based on MARC field and subfield information.
    This function is intended to be used as a helper function for sort_columns.

    Parameters:
    - column_name: The name of the column.

    Returns:
    - A tuple representing the sorting key.
    """
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

def sort_columns(columns):
    """
    Sorts columns based on their sorting keys.

    Parameters:
    - columns: A list of column names.

    Returns:
    - A sorted list of column names.
    """
    return sorted(columns, key=sort_key)

def generate_column_names(field_name, subfield_code, internal_count, subfield_count):
    """
    Generates column names for the DataFrame based on MARC field and subfield information.
    The column names indicate the repetition of fields and subfields.

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