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
