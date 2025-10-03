import pandas as pd
import csv
import os
import ast

from utils.file_picker import select_file
from utils.time_frame import select_window_cli

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

# Static mapping of Beumer Destination IDs to Amazon conventions
MAP_BEUMER_TO_AMAZON = {
    0: "S01017",                                                                                                                                                                                 
    2: "S01019",                                                                                                                                                                                 
    4: "S01021",
    6: "S01023",
    8: "S01025",
    10: "S01027",
    12: "S01029",
    14: "S01031",
    16: "S01033",
    17: "S01034",
    18: "S01035",
    19: "S01036",
    20: "S01037",
    21: "S01038",
    22: "S01039",
    23: "S01040",
    24: "S01041",
    25: "S01042",
    26: "S01043",
    27: "S01044",
    28: "S01045",
    29: "S01046",
    30: "S01047",
    31: "S01048",
    32: "S01049",
    33: "S01050",
    34: "S01051",
    35: "S01052",
    36: "S01053",
    37: "S01054",
    38: "S01055",
    39: "S01056",
    40: "S01057",
    41: "S01058",
    42: "S01059",
    43: "S01060",
    44: "S01061",
    45: "S01062",
    46: "S01063",
    47: "S01064",
    48: "S01065",
    49: "S01066",
    50: "S01067",
    51: "S01068",
    52: "S01069",
    53: "S01070",
    54: "S01071",
    55: "S01072",
    56: "S01073",
    57: "S01074",
    58: "S01075",
    59: "S01076",
    60: "S01077",
    61: "S01078",
    62: "S01079",
    63: "S01080",
    64: "S01080",
    65: "S01082",
    66: "S01083",
    67: "S01084",
    68: "S01085",
    69: "S01086",
    70: "S01087",
    71: "S01088",
    72: "S01089",
    73: "S01090",
    76: "S01091",
    92: "S01093",
    93: "S01094",
    94: "S01095",
    95: "S01096",
    96: "S01097",
    97: "S01098",
    98: "S01099",
    99: "S01100",
    100: "S01101",
    101: "S01102",
    102: "S01103",
    103: "S01104",
    104: "S01105",
    105: "S01106",
    106: "S01107",
    107: "S01108",
    108: "S01109",
    109: "S01110",
    110: "S01111",
    111: "S01112",
    112: "S01113",
    113: "S01114",
    114: "S01115",
    115: "S01116",
    116: "S01117",
    117: "S01118",
    118: "S01119",
    119: "S01120",
    120: "S01121",
    121: "S01122",
    122: "S01123",
    123: "S01124",
    124: "S01125",
    125: "S01126",
    126: "S01127",
    127: "S01128",
    128: "S01129",
    129: "S01130",
    130: "S01131",
    131: "S01132",
    132: "S01133",
    133: "S01134",
    134: "S01135",
    135: "S01136",
    136: "S01137",
    137: "S01138",
    138: "S01139",
    139: "S01140",
    140: "S01141",
    141: "S01142",
    142: "S01143",
    143: "S01144",
    144: "S01145",
    145: "S01146",
    146: "S01147",
    147: "S01148",
    148: "S01149",
    149: "S01150",
    150: "S01151",
    151: "S01152",
    152: "S01153",
    153: "S01154",
    154: "S01155",
    155: "S01156",
    156: "S01157",
    157: "S01158",
    158: "S01159",
    159: "S01160",
    160: "S01161",
    161: "S01162",
    162: "S01163",
    163: "S01164",
    164: "S01165",
    165: "S01166",
    166: "S01167",
    167: "S01168",
    168: "S01169",
    169: "S01170",
    170: "S01171",
    171: "S01172",
    176: "S01173",
    177: "S01174",
    178: "S01175",
    179: "S01176",
    180: "S01177",
    181: "S01178",
    182: "S01179",
    183: "S01180",
    184: "S01181",
    185: "S01182",
    186: "S01183",
    187: "S01184",
    188: "S01185",
    189: "S01186",
    190: "S01187",
    191: "S01188",
    192: "S01189",
    193: "S01190",
    194: "S01191",
    195: "S01192",
    196: "S01193",
    197: "S01194",
    198: "S01195",
    199: "S01196",
    200: "S01197",
    201: "S01198",
    202: "S01199",
    203: "S01200",
    204: "S01201",
    205: "S01202",
    206: "S01203",
    207: "S01204",
    208: "S01205",
    209: "S01206",
    210: "S01207",
    211: "S01208",
    212: "S01209",
    213: "S01210",
    214: "S01211",
    215: "S01212",
    216: "S01213",
    217: "S01214",
    218: "S01215",
    219: "S01216",
    220: "S01217",
    221: "S01218",
    222: "S01219",
    223: "S01220",
    224: "S01221",
    225: "S01222",
    226: "S01223",
    227: "S01224",
    228: "S01225",
    229: "S01226",
    230: "S01227",
    240: "S01229",
    241: "S01230",
    242: "S01231",
    243: "S01232",
    244: "S01233",
    245: "S01234",
    246: "S01235",
    247: "S01236",
    248: "S01237",
    249: "S01238",
    250: "S01239",
    251: "S01240",
    256: "S01015",
    258: "S01013",
    260: "S01011",
    262: "S01009",
    264: "S01007",
    266: "S01005",
    268: "S01003",
    270: "S01001",
    3001: "Recirculation",
    3002: "Recirculation",
}

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

# Drop rows that contain -1 in of the exploded columns
parsed_df = parsed_df[~parsed_df[columns_with_arrays].isin([-1]).any(axis=1)]

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

# Mapping columns to have human-readable values
window_df["sortReason"] = window_df["sortCode"].map(SORT_CODE_MAP)
window_df["defectCategory"] = window_df["sortReason"].map(DEFECT_CATEGORY_MAP)
window_df["Amazon_Destination"] = window_df["requestedDestMCID"].map(MAP_BEUMER_TO_AMAZON)

# Sort Code Analysis
# Count items by sortReason
sort_counts = window_df.groupby("sortReason").size().reset_index(name="count").sort_values(by="count", ascending=False).reset_index(drop=True)
print(sort_counts)

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

# Unique Packages and Recirculation Analysis
# Total packages processed (all rows)
total_processed = len(window_df)

# Total unique barcodes
total_unique_packages = window_df["barcodeAWCS"].nunique()

# Count occurrences of each barcode
counts = window_df["barcodeAWCS"].value_counts()
recirculation_packages = (counts > 1).sum()
# Recirculation = barcodes that appear more than once
recirculation_records = counts[counts > 1].sum()

# Non-recirculation = barcodes that appear only once
non_recirculation_records = counts[counts == 1].sum()

print("Total records:", total_processed)
print("Total unique packages:", total_unique_packages)
print("Total recirculation packages:", recirculation_records)
print("Total non-recirculation packages:", non_recirculation_records)

print("\nHow many barcodes appear exactly once:", (counts == 1).sum())
print("How many barcodes appear more than once:", (counts > 1).sum())
print("Max occurrences for a single barcode:", counts.max())
print("Min occurrences for a single barcode:", counts.min())

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
    ws.write("A3", "Total processed packages:")
    ws.write_number("B3", total_unique_packages)
    ws.write("A4", "Total truly one-off packages:")
    ws.write_number("B4", non_recirculation_records)
    ws.write("A5", "Total recirculation packages:")
    ws.write_number("B5", recirculation_packages)
    ws.write("A6", "Total recirculation records:")
    ws.write_number("B6", recirculation_records)

    ws.write("A8", "Time window", bold)
    ws.write("A9", "StartTime:")
    ws.write("B9", start_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])  # trim to ms
    ws.write("A10", "EndTime:")
    ws.write("B10", end_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
    
    # Defect Category Breakdown
    ws.write("A12", "Defect Category Breakdown (percent over ALL processed)", bold)
    defect_summary.to_excel(writer, sheet_name="Analysis_Results", startrow=12, startcol=0, index=False)

    # Sort Code Reason Counts
    start_row_sort = 12 + len(defect_summary) + 3
    ws.write(start_row_sort - 1, 0, "Sort Code Reason Counts", bold)
    sort_counts.to_excel(writer, sheet_name="Analysis_Results", startrow=start_row_sort, startcol=0, index=False)

    # SortReason vs RequestedDestMCID
    ws.write(30, 0, "SortReason vs RequestedDestMCID", bold)
    reason_dest_pivot.to_excel(
        writer,
        sheet_name="Analysis_Results",
        startrow=31,
        startcol=0,
        index=False
    )

    # Creating native charts
    chart_pie = wb.add_chart({"type": "pie"})   # type: ignore[attr-defined]
    end_row_def = 12 + len(defect_summary)

    chart_pie.add_series({
        "name": "Defect Category Breakdown",
        "categories": ["Analysis_Results", 13, 0, end_row_def, 0],   # defectCategory
        "values": ["Analysis_Results", 13, 1, end_row_def, 1],       # count column
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

    ws.insert_chart(0, 16, bar_chart, {"x_scale": 1.5, "y_scale": 1.5})

    # Define pivot table location
    start_row_pivot = 31
    start_col_pivot = 0
    end_row_pivot = start_row_pivot + len(reason_dest_pivot)
    end_col_pivot = start_col_pivot + len(reason_dest_pivot.columns) - 1

    # Create a stacked column chart
    stack_chart = wb.add_chart({"type": "column", "subtype": "stacked"})    # type: ignore[attr-defined]

    # Loop over each Amazon chute (row of pivot table, skip header row)
    for r in range(start_row_pivot + 1, end_row_pivot + 1):
        stack_chart.add_series({
            "name":       ["Analysis_Results", r, start_col_pivot],  # Amazon chute name
            "categories": ["Analysis_Results", start_row_pivot, start_col_pivot + 1,
                        start_row_pivot, end_col_pivot],          # SortReason labels (header row)
            "values":     ["Analysis_Results", r, start_col_pivot + 1,
                        r, end_col_pivot],                        # Counts across SortReasons
        })

    # Format chart
    stack_chart.set_title({"name": "Amazon Induction vs SortReason"})
    stack_chart.set_x_axis({"name": "SortReason"})
    stack_chart.set_y_axis({"name": "Count"})
    stack_chart.set_legend({"position": "bottom"})

    ws.insert_chart(start_row_pivot + len(reason_dest_pivot) + 1, 0, stack_chart, {"x_scale": 8, "y_scale": 3})

    # Other Sheets
    raw_df.to_excel(writer, sheet_name="Raw_Data", index=False)
    clean_df.to_excel(writer, sheet_name="Clean_Data", index=False)
    window_df.to_excel(writer, sheet_name="Window_Data", index=False)
    export_df.to_excel(writer, sheet_name="Scan_Defects", index=False)

print(f"Analysis results saved to: {output_path}")
