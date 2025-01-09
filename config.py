# config.py
DEFAULT_FOLDER_PATH = ""
#look up OS and sys to find the home folder file path and populate this.
MAX_REPEATS = 15
# this allows for the tabular conversion to skip/drop records as it processes them, to reduce the number of columns generated in the tabular data.
# This is necessary at times when subfields might be repeated several dozen, or several hundred times.
# The conversion process will drop those records into a log file with the 001 as a UID for later examination.