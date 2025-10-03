import tkinter as tk
from tkinter import filedialog
import pandas as pd
import csv
import os

from time_utils import select_window_cli

# Global Constants
WINDOW_TIME = 30  # minutes
MESSAGE_CODE_FILTER = '54163'  # Items Inducted

def select_file():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(
        title="Select File",
        filetypes=[
            ("CSV files", "*.csv"),
            ("Excel files", "*.xlsx;*.xls"),
            ("All files", "*.*"),
        ]
    )

    return file_path
print("Select a S02 data file (CSV format) from Log Monitor...")
path = select_file()

raw_df = pd.read_csv(
    path,
    sep=";",
    header=None,
    engine="python",
    quoting=csv.QUOTE_NONE,
    skipinitialspace=True,
    on_bad_lines="skip",
    dtype=str
)

# Parsing raw data
temp_df = raw_df.replace('"', '',regex=True)        # Remove all double quotes
temp_df = temp_df.replace(r"\s+", '',regex=True)    # Remove all whitespace

temp_df.columns = [
    "timeStamp", "flag", "systemName", "ipAddress", "sender", "unkown",
    "unkown_2", "timeStampPLC", "mainCabinetName", "messageCode", "sequenceNo",
    "rawMessage"
]

# timeStamp parsing
temp_df["timeStamp"] = pd.to_datetime(temp_df["timeStamp"], format="%y%m%d%H%M%S%f", errors="coerce")

# Droping records that are not "54163" (S02) in messageCode column
original_records = len(temp_df)
temp_df = temp_df[temp_df["messageCode"] == MESSAGE_CODE_FILTER]
remaining_records = len(temp_df)
dropped_count = original_records - remaining_records
print(f"Filtered dataset: kept {remaining_records} rows with messageCode = {MESSAGE_CODE_FILTER} "
      f"\n\tdropped {dropped_count} out of {original_records} total rows")

# Helper functions to handle arrays inside values
def split_key_values(text):
    """Split key:value pairs by commas, ignoring commas inside [brackets]."""
    parts = []                 # Final list of key:value strings
    buf = ""                   # Temporary buffer to collect characters
    inside_brackets = 0        # Counter to track nesting depth of [ ]

    for ch in text:
        if ch == "[": 
            inside_brackets += 1   # Entering a bracket → increase depth
        elif ch == "]": 
            inside_brackets -= 1   # Leaving a bracket → decrease depth

        # Split only on commas that are *outside* brackets
        if ch == "," and inside_brackets == 0:
            parts.append(buf.strip())  # Save the current piece
            buf = ""                   # Reset buffer for next piece
        else:
            buf += ch                  # Keep building the current piece

    # Append the last piece (after the loop ends)
    if buf:
        parts.append(buf.strip())

    return parts

def parse_row(text):
    """Convert a rawMessage string into a dictionary of key:value pairs."""
    key_value_strings = split_key_values(text)   # Split into ["key1:value1", "key2:value2", ...]
    parsed_dict = {}                             # Dictionary to hold final result

    for pair in key_value_strings:
        if ":" in pair:                          # Only process well-formed pairs
            key, value = pair.split(":", 1)      # Split into key and value (only on the first colon)
            parsed_dict[key.strip()] = value.strip()  # Clean whitespace and store

    return parsed_dict

def first_element(val):
    """Extract the first element from a string representation of a list."""
    if isinstance(val, str) and val.startswith("[") and val.endswith("]"):
        raw_elements = val.strip("[]").split(",")  # Break into parts
        parts = []
        for element in raw_elements:
            parts.append(element.strip())          # Clean whitespace and collect
        return parts[0] if parts else val          # Return first element if available
    return val

# Message Column parsing
temp_df["rawMessage"] = temp_df["rawMessage"].str.removeprefix("->{").str.removesuffix("}<")
# Expand rawMessage into columns
# Keeping in mind that some values are lists enclosed in [ ]
message_df = temp_df["rawMessage"].apply(parse_row).apply(pd.Series)
# Extract first element from list-like values
special_columns = ["requestedDestMCID", "sortCode", "requestedDestStatus"]
for col in special_columns:
    message_df[col] = message_df[col].apply(first_element)

# Convert sortCode to integer (nullable type)
message_df["sortCode"] = pd.to_numeric(message_df["sortCode"], errors="coerce").astype("Int64")

# Join parsed message columns with the base dataframe
parsed_df = pd.concat([temp_df.drop(columns=["rawMessage"]), message_df], axis=1)

# Cleaning DataFrame
# Get list of columns with only 1 unique value, but preserve "sortCode", "indexNo" and "timeStamp"
cols_to_drop = parsed_df.columns[parsed_df.nunique() == 1].tolist()
for col in ["indexNo", "timeStamp"]:   # don’t drop sortCode or indexNo
    if col in cols_to_drop:
        cols_to_drop.remove(col)
# Usual Columns Dropped
# ['flag', 'systemName', 'ipAddress', 'sender', 'unkown', 'unkown_2', 'machineCode', 'unitID', 'event', 'requestedDestStatus', 'comHost', 'comMode', 'telegramType']

clean_df = parsed_df.drop(columns=cols_to_drop)
# Usual Columns Remaining
# ['timeStamp', 'PLCTimeStamp', 'sequenceNo', 'plcRecordNo', 'itemID', 'indexNo', 'locationAWCS', 'barcodeAWCS', 'actualDestMCID', 'requestedDestMCID', 'sortCode']

# S02 Analysis
print("Select time window for analysis:")
window_df, start_ts, end_ts = select_window_cli(clean_df, WINDOW_TIME)

window_df_unique = window_df.drop_duplicates(subset=["barcodeAWCS"], keep="first")

# Total packages processed (all rows)
total_processed = len(window_df_unique)

# Count of each unique value in requestedDestMCID
dest_counts = window_df_unique["requestedDestMCID"].value_counts().reset_index()
dest_counts.columns = ["requestedDestMCID", "Count"]

# Count values in barcodeAWCS
barcode_counts = window_df["barcodeAWCS"].value_counts()

# Filter only repeated barcodes (count > 1)
repeated_barcodes = barcode_counts[barcode_counts > 1].reset_index()
repeated_barcodes.columns = ["BarcodeAWCS", "Count"]

# Total unique packages processed (based on barcodeAWCS)
unique_packages = window_df_unique["barcodeAWCS"].nunique()

# Ensure "data" folder exists
os.makedirs("data", exist_ok=True)
# Exporting to Excel file
start_str = start_ts.strftime("%Y%m%d-%H%M%S")
end_str   = end_ts.strftime("%Y%m%d-%H%M%S")
output_path = f"data/Analysis_S02_{start_str}_{end_str}.xlsx"

print("\nExporting analysis results to Excel file...")
with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
    wb = writer.book
    ws = wb.add_worksheet("Analysis_Results")   # type: ignore[attr-defined]
    bold = wb.add_format({"bold": True})        # type: ignore[attr-defined]
    
    # Analysis Summary
    ws.write("A1", "Analysis Summary", bold)
    ws.write("A2", "Total records (window dataset):")
    ws.write_number("B2", total_processed)
    
    ws.write("A3", "Time window", bold)
    ws.write("A4", "StartTime:")
    ws.write("B4", start_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])  # trim to ms
    ws.write("A5", "EndTime:")
    ws.write("B5", end_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])

    # Add to Analysis Summary
    ws.write("A6", "Number of unique packages processed:")
    ws.write_number("B6", unique_packages)

    # Write headers
    ws.write("A7", "RequestedDestMCID", bold)
    ws.write("B7", "Count", bold)

    # Write values
    for i, (dest, count) in enumerate(zip(dest_counts["requestedDestMCID"], dest_counts["Count"]), start=8):
        ws.write(i - 1, 0, dest)   # column A
        ws.write(i - 1, 1, count)  # column B

    # Define chart range (start row = 7, end row = 7 + len(dest_counts))
    end_row = 6 + len(dest_counts)  # 0-based index for xlsxwriter

    # bar chart with vertical columns
    chart_bar = wb.add_chart({"type": "column"})   # type: ignore[attr-defined]   # or "column" if you prefer vertical bars
    chart_bar.add_series({
        "name": "RequestedDestMCID Breakdown",
        "categories": ["Analysis_Results", 7, 0, end_row, 0],  # RequestedDestMCID values
        "values": ["Analysis_Results", 7, 1, end_row, 1],      # Counts
        "data_labels": {"value": True},
    })

    chart_bar.set_title({"name": "RequestedDestMCID Breakdown"})
    chart_bar.set_x_axis({"name": "Count"})
    chart_bar.set_y_axis({"name": "RequestedDestMCID"})
    chart_bar.set_style(11)

    # Insert chart in sheet (e.g., at cell D7)
    ws.insert_chart("D7", chart_bar, {"x_scale": 3, "y_scale": 3})
    
    # Other Sheets
    raw_df.to_excel(writer, sheet_name="Raw_Data", index=False)
    clean_df.to_excel(writer, sheet_name="Clean_Data", index=False)
    window_df_unique.to_excel(writer, sheet_name="Window_Data", index=False)

    ws_repeated = wb.add_worksheet("Repeated_Barcodes")   # type: ignore[attr-defined]
    # Write headers
    ws_repeated.write("A1", "BarcodeAWCS", bold)
    ws_repeated.write("B1", "Count", bold)

    # Write repeated barcode values
    for i, (barcode, count) in enumerate(zip(repeated_barcodes["BarcodeAWCS"], repeated_barcodes["Count"]), start=2):
        ws_repeated.write(i - 1, 0, barcode)
        ws_repeated.write(i - 1, 1, count)

print(f"Analysis results saved to: {output_path}")
