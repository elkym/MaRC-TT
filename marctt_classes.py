### Classes for field comparison in Diagnostic Tool ###
import warnings
import pandas as pd
from marc_diag_utils import compare_strings

# Parent Class for Field Comparators
class FieldComparator:
    def compare(self, marc_value, df_value):
        return compare_strings(str(marc_value), str(df_value))

# Class for Static Field Comparison
class StaticFieldComparator(FieldComparator):
    def compare_field(self, record, df_row, marc_field, df_field):
        marc_value = str(record[marc_field].value()) if record[marc_field] else ''
        df_value = str(df_row.get(df_field, ''))
        return self.compare(marc_value, df_value)

# Class for Dynamic Field Comparison
class DynamicFieldComparator(FieldComparator):
    def compare_field(self, record, df_row, marc_field, subfields):
        for field in record.get_fields(marc_field):
            for subfield in subfields:
                for i, subfield_value in enumerate(field.get_subfields(subfield), start=1):
                    marc_value = str(subfield_value)
                    df_field_name = f'{marc_field}.{i}.{subfield}'
                    df_value = str(df_row.get(df_field_name, ''))
                    if not self.compare(marc_value, df_value):
                        return False
        return True
    
# Class for Field Comparison with Subfield Indicators
class IndicatorValidator:
    def __init__(self):
        pass

    def validate_marc_indicators(self, record):
        marc_invalid_indicators_data = []
        for field in record.fields:
            if len(field.indicators) != 2:
                marc_invalid_indicators_data.append((field.tag, field.indicators))
            else:
                for indicator in field.indicators:
                    if not (indicator.isdigit() or indicator in [' ', '#', '\\'] or indicator == '##' or indicator == '\\\\'):
                        marc_invalid_indicators_data.append((field.tag, field.indicators))
                    if '#' in indicator:
                        warnings.warn(f"Potential data corruption detected in MaRC data, field {field.tag} with indicator {indicator}", UserWarning)
        return marc_invalid_indicators_data

    def validate_tabular_indicators(self, df_map):
        valid_tabular_inds_len2 = ['  ', '##', '\\\\']
        other_valid_inds = ['', ' ']
        invalid_indicators = []
        for uid, row in df_map.items():
            for col in row.index:
                if col.endswith('.IND'):
                    indicator = str(row[col])
                    # 1st if: data not longer than 2 digits
                    if len(indicator) > 2:
                        invalid_indicators.append((uid, col, indicator))
                        continue
                    # 2nd if: data not matching elements of the lists of valid indicators
                    if indicator not in valid_tabular_inds_len2 and indicator not in other_valid_inds:
                        # 3rd if: data contains characters other than digits and/or hashtags and backslashes
                        if not ((indicator.isdigit() and len(indicator) == 2) or 
                                (len(indicator) > 1 and indicator[0].isdigit() and indicator[1] in ['#', '\\']) or 
                                (len(indicator) > 1 and indicator[1].isdigit() and indicator[0] in ['#', '\\'])):                            
                            # Handle single digit case
                            if len(indicator) == 1 and indicator != ' ':
                                invalid_indicators.append((uid, col, indicator))
                            else:
                                invalid_indicators.append((uid, col, indicator))
        return invalid_indicators
    
class DataFrameSaver:
    def __init__(self, default_path=None):
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
        try:
            df.to_parquet(file_path, **kwargs)
            print(f"DataFrame saved to {file_path} as .parquet")
        except Exception as e:
            self._handle_error(e, file_path)