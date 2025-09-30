import tkinter as tk
from tkinter import filedialog
import pandas as pd
import csv
import datetime as dt
import os

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
for col in ["sortCode", "indexNo", "timeStamp"]:   # don’t drop sortCode or indexNo
    if col in cols_to_drop:
        cols_to_drop.remove(col)
# Usual Columns Dropped
# ['flag', 'systemName', 'ipAddress', 'sender', 'unkown', 'unkown_2', 'machineCode', 'unitID', 'event', 'requestedDestStatus', 'comHost', 'comMode', 'telegramType']

clean_df = parsed_df.drop(columns=cols_to_drop)
# Usual Columns Remaining
# ['timeStamp', 'PLCTimeStamp', 'sequenceNo', 'plcRecordNo', 'itemID', 'indexNo', 'locationAWCS', 'barcodeAWCS', 'actualDestMCID', 'requestedDestMCID', 'sortCode']

# Rate Analysis
# using time frame window to select a subset of data and preform analysis
global_start_time = clean_df["timeStamp"].min()
global_end_time = clean_df["timeStamp"].max()
global_delta_time = global_end_time - global_start_time

print(f"Start Time: {global_start_time}")
print(f"End Time: {global_end_time}")
print(f"Delta Time: {global_delta_time}\n")

def parse_datetime_or_time(s, default_date):
    """
    Parse a string into a pandas.Timestamp.

    Supports:
    - Time only (e.g. "16:00" → combined with default_date)
    - Hour only (e.g. "16" → 16:00:00 on default_date)
    - Full datetime (e.g. "2025-09-24 16:00")

    Raises ValueError if parsing fails.

    Parameters
    ----------
    s : str
        The user input string (could be time-only, hour-only, or full datetime).
    default_date : datetime-like
        A reference datetime from which the date component will be used when
        `s` contains only a time or hour.

    Returns
    -------
    pd.Timestamp
        A timestamp object combining the parsed time (or datetime) with the
        appropriate date context.
    """
    # Try time-only input like "HH:MM" or "HH:MM:SS"
    try:
        t = dt.time.fromisoformat(s)
        return pd.Timestamp(dt.datetime.combine(default_date.date(), t))
    except ValueError:
        pass
    
    # Try simple hour like '16'
    try:
        t = dt.time(int(s), 0, 0)
        return pd.Timestamp(dt.datetime.combine(default_date.date(), t))
    except (ValueError, TypeError):
        pass
    
     # Try full datetime string like "YYYY-MM-DD HH:MM:SS"
    try:
        return pd.Timestamp(s)
    except ValueError:
        raise ValueError(f"Could not parse '{s}' as time or datetime")

def select_window_cli(df, window_time):
    """
    Prompt the user to select a start and end time window for analysis.

    The function accepts user input as either a full datetime or a time-only
    value. If no input is given, defaults to the dataset start and a window
    of `window_time` minutes. Boundaries are automatically adjusted if the
    requested window falls outside the dataset.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataset containing a 'timeStamp' column.
    window_time : int
        Default duration (in minutes) if no end time is provided.

    Returns
    -------
    win : pandas.DataFrame
        Subset of df within the selected window.
    start : pd.Timestamp
        Resolved start time.
    end : pd.Timestamp
        Resolved end time.
    """
    # Build example inputs for the user prompt based on dataset range
    start_example_full = global_start_time.strftime("%Y-%m-%d %H:%M")
    end_example_full   = global_end_time.strftime("%Y-%m-%d %H:%M")
    start_example_time = global_start_time.strftime("%H:%M")
    end_example_time   = global_end_time.strftime("%H:%M")

    # Get start time from user
    s = input(
        f"Start → Example: '{start_example_time}' (24h format) "
        f"or '{start_example_full}' (full datetime), "
        f"or press Enter to use dataset start: "
    ).strip()
    
    if s:
        # Use the oldest date from dataset when parsing time-only input
        start = parse_datetime_or_time(s, global_start_time)
    else:  # default to dataset start
        start = global_start_time

    # Check and correct start time boundaries
    if start < global_start_time:
        print(f"⚠️  WARNING: Requested start time ({start}) is before data begins ({global_start_time})")
        print(f"   → Adjusting start time to data beginning: {global_start_time}")
        start = global_start_time
    
    if start > global_end_time:
        print(f"❌ ERROR: Requested start time ({start}) is after data ends ({global_end_time})")
        print("   → No data available for this time window")
        return df.iloc[0:0].copy(), start, start  # Return empty dataframe

    # Get end time from user
    e = input(
        f"End   → Example: '{end_example_time}' (24h format) "
        f"or '{end_example_full}' (full datetime), "
        f"or press Enter to use {window_time} min window: "
    ).strip()
    
    if e:
        # User provided an end time - parse it using the corrected start date
        end = parse_datetime_or_time(e, start)
    else:
        # No input - use start time + window
        end = start + pd.Timedelta(minutes=window_time)

    # Check and correct end time boundaries
    if end > global_end_time:
        original_end = end
        end = global_end_time
        actual_window_minutes = (end - start).total_seconds() / 60
        print(f"⚠️  WARNING: Requested end time ({original_end}) exceeds data boundary ({global_end_time})")
        print(f"   → Adjusting end time to data boundary: {global_end_time}")
        print(f"   → Actual window duration: {actual_window_minutes:.1f} minutes (requested: {window_time} minutes)")
    
    # Prevent invalid windows
    if end < start:
        print(f"❌ ERROR: End time ({end}) is before start time ({start})")
        print("   → No valid time window")
        return df.iloc[0:0].copy(), start, end

    # Filter dataframe within the window
    mask = (df["timeStamp"] >= start) & (df["timeStamp"] <= end)
    win = df.loc[mask].copy()
    actual_duration = (end - start).total_seconds() / 60
    print(f"\nWindow: {start} → {end}  ({actual_duration:.1f} min) | Rows: {len(win)}")
    return win, start, end

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

# Recirculation Packages
window_df["actualDestMCID"] = pd.to_numeric(window_df["actualDestMCID"], errors="coerce").astype("Int64")
window_df["requestedDestMCID"] = pd.to_numeric(window_df["requestedDestMCID"], errors="coerce").astype("Int64")

recirc_mask = (
    ((window_df["actualDestMCID"] == 3001) & (window_df["requestedDestMCID"] == 3002))
    | ((window_df["actualDestMCID"] == 3002) & (window_df["requestedDestMCID"] == 3001))
)

recirc_count = recirc_mask.sum()
print(f"\nRecirculation packages: {recirc_count}\n")

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
    ws.write_number(recirculation_row + 1, 1, recirc_count)

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

    # Other Sheets
    raw_df.to_excel(writer, sheet_name="Raw_Data", index=False)
    clean_df.to_excel(writer, sheet_name="Clean_Data", index=False)
    window_df.to_excel(writer, sheet_name="Window_Data", index=False)
    export_df.to_excel(writer, sheet_name="Scan_Defects", index=False)

print(f"Analysis results saved to: {output_path}")
