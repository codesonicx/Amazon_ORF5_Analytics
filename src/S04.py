import tkinter as tk
from tkinter import filedialog
import pandas as pd
import csv
import os
import ast

from time_utils import select_window_cli

# Global Constants
TARGET_SCAN_DEFECT_NO_CROSSBELT = 0.01
TARGET_SCAN_DEFECT_WITH_CROSSBELT = 0.005
TARGET_MH_DEFECT_NO_CROSSBELT = 0.02
TARGET_MH_DEFECT_WITH_CROSSBELT = 0.01
WINDOW_TIME = 30  # minutes
MESSAGE_CODE_FILTER = '54177'  # Items Inducted

# Dictionary for mapping sort codes
SORT_CODE_MAP = {
    0: "Success",
    1: "Unknown",
    2: "Unexpected_Container",
    3: "Tracking_Error",
    4: "Gap_Error",
    5: "Destination_Full",
    6: "Destination_Non_Operational",
    7: "Invalid_Destination",
    8: "No_Read",
    9: "No_Code",
    10: "Multi_Label",
    11: "<reserved>",
    12: "Destination_Disabled",
    13: "Throughput_Limit",
    14: "Failed_To_Divert",
    15: "<reserved>",
    16: "No_Destination_Received",
    17: "Lost_Container",
    18: "Dimension_Error",
    19: "Weight_Error",
    20: "Container_Utilization",
    21: "Unable_To_Divert",
    22: "Destination_Not_Attempted"
}

# Dictionary of defect categories
DEFECT_CATEGORY_MAP = {
    # Scan Defect
    "Multi_Label": "Scan Defect",
    "No_Read": "Scan Defect",
    "No_Code": "Scan Defect",

    # MHE Defect
    "Failed_To_Divert": "MHE Defect",
    "Gap_Error": "MHE Defect",
    "Destination_Non_Operational": "MHE Defect",    # Lane_Non_Operational in doc
    "Lost_Container": "MHE Defect",
    "No_Destination_Received": "MHE Defect",
    "Unknown": "MHE Defect",                        # Sort_Unknown in doc
    "Tracking_Error": "MHE Defect",
    "Unable_To_Divert": "MHE Defect",
}

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
print("Select a S04 data file (CSV format) from Log Monitor...")
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

# Droping records that are not "54177" (S04) in messageCode column
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

def parse_list(val):
    """Convert a string representation of a list into an actual Python list."""
    if isinstance(val, str) and val.startswith("[") and val.endswith("]"):
        try:
            return ast.literal_eval(val)  # Safely evaluate the string to a Python list
        except (ValueError, SyntaxError):
            return []                     # Return empty list on error
    return [val]    # Fallback: wrap non-list in a list

# Message Column parsing
temp_df["rawMessage"] = temp_df["rawMessage"].str.removeprefix("->{").str.removesuffix("}<")
# Expand rawMessage into columns
# Keeping in mind that some values are lists enclosed in [ ]
message_df = temp_df["rawMessage"].apply(parse_row).apply(pd.Series)
# Extract first element from list-like values
columns_with_arrays = ["requestedDestMCID", "sortCode", "requestedDestStatus"]
for col in columns_with_arrays:
    message_df[col] = message_df[col].apply(parse_list)

# Join parsed message columns with the base dataframe
parsed_df = pd.concat([temp_df.drop(columns=["rawMessage"]), message_df], axis=1)

def normalize_lists(row, target_cols):
    max_len = max(len(row[c]) if isinstance(row[c], list) else 0 for c in target_cols)
    for c in target_cols:
        if not isinstance(row[c], list):
            row[c] = [row[c]]
        if len(row[c]) < max_len:
            filler = -1 if c != "requestedDestStatus" else "Unused"
            row[c] = row[c] + [filler] * (max_len - len(row[c]))
    return row

parsed_df = parsed_df.apply(normalize_lists, axis=1, target_cols=columns_with_arrays)

parsed_df = parsed_df.explode(column=columns_with_arrays, ignore_index=True)    # type: ignore[arg-type]

# Cleaning DataFrame
# Get list of columns with only 1 unique value, but preserve "sortCode", "indexNo" and "timeStamp"
cols_to_drop = parsed_df.columns[parsed_df.nunique() == 1].tolist()
for col in ["sortCode", "indexNo", "timeStamp"]:   # don’t drop sortCode or indexNo
    if col in cols_to_drop:
        cols_to_drop.remove(col)
# Usual Columns Dropped
# ['flag', 'systemName', 'ipAddress', 'sender', 'unkown', 'unkown_2', 'machineCode', 'unitID', 'event', 'requestedDestStatus', 'comHost', 'comMode', 'telegramType']

clean_df = parsed_df.drop(columns=cols_to_drop)
# Usual Columns Remaining
# ['timeStamp', 'PLCTimeStamp', 'sequenceNo', 'plcRecordNo', 'itemID', 'indexNo', 'locationAWCS', 'barcodeAWCS', 'actualDestMCID', 'requestedDestMCID', 'sortCode']

# Rate Analysis
print("Select time window for analysis:")
window_df, start_ts, end_ts = select_window_cli(clean_df, WINDOW_TIME)

# Optional cleanup of wrong sortCodes
do_cleanup = input("Do you want to clean up wrong sortCodes using the Excel file? (yes/no): ").strip().lower()

if do_cleanup != "yes":
    print("\nSkipping sortCode cleanup step.\n")
else:
    bad_ids_path = select_file()
    if not bad_ids_path:
        print("\nNo file selected. Skipping Cleanup\n")
    else:
        # Load file depending on extension
        if bad_ids_path.endswith(".csv"):
            bad_ids_df = pd.read_csv(bad_ids_path)
        elif bad_ids_path.endswith((".xlsx", ".xls")):
            bad_ids_df = pd.read_excel(bad_ids_path)
        else:
            raise ValueError("Unsupported file type selected!")

        # First two columns: ID and Comment
        id_col = bad_ids_df.columns[0]
        comment_col = bad_ids_df.columns[1]

        # Build {indexNo -> comment} with 4-digit padding
        id_comment_dict = {}
        for _, row in bad_ids_df.iterrows():
            if pd.notna(row[id_col]):
                key = str(int(row[id_col])).zfill(4)
                comment = row[comment_col] if pd.notna(row[comment_col]) else ""
                id_comment_dict[key] = comment

        bad_set = set(id_comment_dict.keys())

        # Ensure explanation column exists
        if "No Scan Defect Explanation" not in window_df.columns:
            window_df["No Scan Defect Explanation"] = ""

        # Restrict to scan-defect rows
        scan_defects = window_df[window_df["sortCode"].isin([8, 9, 10])]

        modified_count = 0
        matched_ids = []

        # Keep track of which rows we've already modified
        used_rows = set()

        for bad_id, comment in zip(bad_ids_df[id_col], bad_ids_df[comment_col]):
            if pd.isna(bad_id):
                continue
            padded_id = str(int(bad_id)).zfill(4)

            # Find candidate rows not already used
            candidates = scan_defects.index[
                (scan_defects["indexNo"] == padded_id) & (~scan_defects.index.isin(used_rows))
            ]

            if len(candidates) > 0:
                row_idx = candidates[0]   # take the next available one
                window_df.at[row_idx, "sortCode"] = 0
                window_df.at[row_idx, "No Scan Defect Explanation"] = comment if pd.notna(comment) else ""
                used_rows.add(row_idx)

                matched_ids.append(padded_id)
                modified_count += 1

        # IDs from user list that didn’t get applied
        not_found = [str(int(x)).zfill(4) for x in bad_ids_df[id_col] if str(int(x)).zfill(4) not in matched_ids]

        print(f"Modified sortCode to 0 for {modified_count} rows (respecting duplicates in user list).")
        if matched_ids:
            print("IDs modified:", matched_ids)
        if not_found:
            print("IDs not applied (no scan-defect row left):", not_found)

# Sort Code Analysis
# Map sortCode to sortReason and defectCategory columns
window_df["sortReason"] = window_df["sortCode"].map(SORT_CODE_MAP)
window_df["defectCategory"] = window_df["sortReason"].map(DEFECT_CATEGORY_MAP)
# Count items by sortReason
sort_counts = window_df.groupby("sortReason").size().reset_index(name="count").sort_values(by="count", ascending=False).reset_index(drop=True)
print(sort_counts)

# Read mapping file, force Buemer to string
map_df = pd.read_excel(
    r"C:\Users\joacosta\Work\Beumer\NortFord\Python\ORF5\data\Conventions\chutes_name_mapping.xlsx",
    dtype={"Buemer": str, "Amazon": str}
)

# Build dictionary with string keys
map_dict = dict(zip(map_df["Buemer"], map_df["Amazon"]))

# Ensure requestedDestMCID is string before mapping
window_df["Amazon_Destination"] = window_df["requestedDestMCID"].astype(str).apply(
    lambda x: map_dict.get(x, f"BeumerName:{x}")
)

# Breakdown of sortReason vs Amazon_Destination
reason_dest_summary = (
    window_df.groupby(["sortReason", "Amazon_Destination"])
    .size()
    .reset_index(name="count")
    .sort_values(by=["sortReason", "count"], ascending=[True, False])
    .reset_index(drop=True)
)

# SortReason vs Amazon_Destination (pivot table)
reason_dest_pivot = reason_dest_summary.pivot_table(
    index="sortReason",
    columns="Amazon_Destination",
    values="count",
    fill_value=0
).reset_index()

# ---- Unique Packages and Recirculation Analysis ----
# Unique packages (distinct barcodes)
unique_packages = window_df["barcodeAWCS"].nunique()
# Count occurrences of each barcode
barcode_counts = window_df["barcodeAWCS"].value_counts()
# Recirculating packages (those that appear more than once)
recirc_packages = barcode_counts[barcode_counts > 1]
# Number of unique packages that recirculated
recirc_unique_packages = len(recirc_packages)

# Total recirculation events (extra passes beyond the first)
recirc_events = (recirc_packages - 1).sum()

print(f"Unique packages processed: {unique_packages}")
print(f"Packages that recirculated (unique barcodes): {recirc_unique_packages}")
print(f"Total recirculation events (extra scans): {recirc_events}")

# Total packages processed (all rows)
total_processed = len(window_df)

# Count defects only (exclude NaN)
defect_summary = (
    window_df["defectCategory"]
    .value_counts(dropna=True)
    .rename_axis("defectCategory")
    .reset_index(name="count")
)

# Adding "No Defect" row
defect_count_total = defect_summary["count"].sum()
no_defect_count = total_processed - defect_count_total

defect_summary = pd.concat(
    [defect_summary, pd.DataFrame([{"defectCategory": "No Defect", "count": no_defect_count}])],
    ignore_index=True
)

# Percent over total processed
defect_summary["percentage"] = (defect_summary["count"] / total_processed * 100).round(4)
print(defect_summary)

# Filter for sortCode 8, 9, and 10 (Scan Defects) using the helper column 'defectCategory'
scan_defect_df = window_df[window_df['defectCategory'] == "Scan Defect"]
print("\nBreakdown by Scan Defect:")
print(f"Found '{len(scan_defect_df)}' items with sortCode 8, 9, or 10 (Scan Defects)")

# Create a new dataset with just the columns we need
export_df = scan_defect_df[['indexNo', 'timeStamp', 'sortCode']].copy()

# Ensure "data" folder exists
os.makedirs("data", exist_ok=True)
# Exporting to Excel file
start_str = start_ts.strftime("%Y%m%d-%H%M%S")
end_str   = end_ts.strftime("%Y%m%d-%H%M%S")
output_path = f"data/Analysis_SO4_{start_str}_{end_str}.xlsx"

print("\nExporting analysis results to Excel file...")
with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
    wb = writer.book
    ws = wb.add_worksheet("Analysis_Results")   # type: ignore[attr-defined]
    bold = wb.add_format({"bold": True})        # type: ignore[attr-defined]
    
    # Analysis Summary
    ws.write("A1", "Analysis Summary", bold)
    ws.write("A2", "Total records (window dataset):")
    ws.write_number("B2", total_processed)
    ws.write_number("C2", unique_packages)
    
    ws.write("A3", "Time window", bold)
    ws.write("A4", "StartTime:")
    ws.write("B4", start_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])  # trim to ms
    ws.write("A5", "EndTime:")
    ws.write("B5", end_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
    
    # Defect Category Breakdown
    ws.write("A7", "Defect Category Breakdown (percent over ALL processed)", bold)
    defect_summary.to_excel(writer, sheet_name="Analysis_Results", startrow=8, startcol=0, index=False)

    # Sort Code Reason Counts
    start_row_sort = 8 + len(defect_summary) + 3
    ws.write(start_row_sort - 1, 0, "Sort Code Reason Counts", bold)
    sort_counts.to_excel(writer, sheet_name="Analysis_Results", startrow=start_row_sort, startcol=0, index=False)

    # Recirculation Packages
    recirculation_row = start_row_sort + len(sort_counts) + 2
    ws.write(recirculation_row, 0, "Recirculation Packages", bold)
    ws.write(recirculation_row + 1, 0, "Count:")
    ws.write_number(recirculation_row + 1, 1, recirc_unique_packages)

    # Creating native charts
    chart_pie = wb.add_chart({"type": "pie"})   # type: ignore[attr-defined]
    end_row_def = 8 + len(defect_summary)

    chart_pie.add_series({
        "name": "Defect Category Breakdown",
        "categories": ["Analysis_Results", 9, 0, end_row_def, 0],   # defectCategory
        "values": ["Analysis_Results", 9, 1, end_row_def, 1],       # count column
        "data_labels": {
            "percentage": True,
            "num_format": "0.0%",
            "position": "outside_end"
        },
    })
    chart_pie.set_title({"name": "Defect Breakdown"})
    chart_pie.set_legend({"position": "right"})

    ws.insert_chart(0, 4, chart_pie, {"x_scale": 1.5, "y_scale": 1.5})

    bar_chart = wb.add_chart({"type": "column"})    # type: ignore[attr-defined]
    end_row_sort = start_row_sort + len(sort_counts)
    bar_chart.add_series({
        "name": "Sort Code Reason Counts",
        "categories": ["Analysis_Results", start_row_sort + 1, 0, end_row_sort, 0],
        "values": ["Analysis_Results", start_row_sort + 1, 1, end_row_sort, 1],
        "data_labels": {"value": True},
    })
    bar_chart.set_title({"name": "Items per Sort Code Reason"})
    bar_chart.set_x_axis({"name": "Sort Reason"})
    bar_chart.set_y_axis({"name": "Number of Items"})
    bar_chart.set_legend({"none": True})

    ws.insert_chart(25, 0, bar_chart, {"x_scale": 1.5, "y_scale": 1.5})

    start_row_pivot = 12 + len(reason_dest_summary) + 3
    ws.write(start_row_pivot - 1, 0, "SortReason vs RequestedDestMCID", bold)
    reason_dest_pivot.to_excel(
        writer,
        sheet_name="Analysis_Results",
        startrow=start_row_pivot,
        startcol=0,
        index=False
    )

    # Other Sheets
    #raw_df.to_excel(writer, sheet_name="Raw_Data", index=False)
    #clean_df.to_excel(writer, sheet_name="Clean_Data", index=False)
    window_df.to_excel(writer, sheet_name="Window_Data", index=False)
    export_df.to_excel(writer, sheet_name="Scan_Defects", index=False)

print(f"Analysis results saved to: {output_path}")
