# Configuration values
MAX_REPEATS = 15

# Default folder path settings for file dialogs
'''Diagnostic logs, saved files, etc., will be placed here.
The user can specify this file path, or set USE_DEFAULT_FOLDER to False,
in which case, file saving methods will prompt the user
with a tkinter file selection window. 
'''
DEFAULT_FOLDER_PATH = ''
USE_DEFAULT_FOLDER = False

# Constants for MaRC and tabular data comparison
'''
These constants are used in the diagnostic tool to compare MaRC and tabular data.
SET_UID is currently not implemented.
'''
SET_LDR = 'LDR'
SET_UID = '001'

# Control fields are required for several scripts
'''
These fields lack indicators and are handled differently than other fields.
SET_LDR is the Leader field, which is always the first field in a MaRC record.
Sometimes this appears as '000', other times as 'LDR'.
'''
CONTROL_FIELDS = [SET_LDR, '001', '003', '005', '006', '007', '008']

# Error constants (See marc_diagnostic_main.py)
'''
These constants are used in the diagnostic tool to log errors.
Flattening and unflattening the data may require reports on inconsistencies.
This list may grow as the diagnostic tool is developed further.
'''
ERR_NON_DIGIT_UID = 'ERR001: Non-digit characters in UID'
ERR_RECORD_NOT_FOUND = 'ERR002: Record not found in Excel'
ERR_DUPLICATE_CONTROL_FIELD = 'ERR003: Duplicate Control Field'
ERR_NON_UTF8_ENCODING = 'ERR004: Non-UTF-8 encoding detected in field/subfield'
ERR_MISSING_UID = 'ERR005: Missing UID in MARC record'
ERR_EXCESSIVE_FIELD_REPETITIONS = 'ERR006: Excessive Field Repetitions'
ERR_STRING_MISMATCH = 'ERR007: String mismatch'
ERR_INVALID_INDICATOR = 'ERR008: Invalid indicator'

# Fields to compare for string comparison in MARC and tabular data (See marc_diagnostic_main.py)
FIELDS_TO_COMPARE = {
    "LDR": "LDR.1",
    "003": "003.1.",
    "008": "008.1.",
    "952": ["c"]
}