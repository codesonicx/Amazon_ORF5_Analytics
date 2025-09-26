import tkinter as tk
from tkinter import filedialog
import pandas as pd
import csv
import datetime as dt
import math

# Global Constants
TARGET_SINGLE_AUTO_PPH = 2820.0
TARGET_SINGLE_SEMI_AUTO_PPH = 2640.0
TARGET_ALL_AUTO_PPH = 7351.0 / 3.0
TARGET_ALL_SEMI_AUTO_PPH = 9119.0 / 4.0
WINDOW_TIME = 30  # minutes
MESSAGE_CODE_FILTER = '54123'  # Items Inducted

# Choose target PPH based on inductions, ignoring others.
SEMI = {"IU001", "IU002", "IU003", "IU004"}
AUTO = {"IU005", "IU006", "IU007"}
SPS_NAMES = {"SPS001", "SPS002"}

def select_file():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(
        title="Select CSV File",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
    )

    root.destroy()  # Close the Tkinter instance
    return file_path

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

# Droping records that are not "54123" (Items Inducted) in messageCode column
original_records = len(temp_df)
temp_df = temp_df[temp_df["messageCode"] == MESSAGE_CODE_FILTER]
remaining_records = len(temp_df)
dropped_count = original_records - remaining_records
print(f"Filtered dataset: kept {remaining_records} rows with messageCode = {MESSAGE_CODE_FILTER} "
      f"\n\tdropped {dropped_count} out of {original_records} total rows")

# Message Column parsing
temp_df["rawMessage"] = temp_df["rawMessage"].str.removeprefix("->{").str.removesuffix("}<")
message_df = temp_df["rawMessage"].str.split(",", expand=True)          # Split into columns
col_names = message_df.iloc[0].str.split(":", n=1).str[0].str.strip()   # Extract keys (before ':') from the first row as column names
message_df.columns = col_names                                          # Assign new column names
message_df = message_df.apply(lambda x: x.str.split(":", n=1).str[1])   # Keep only values (after ':')

# Induction Mapping
induction_map = {
    "0": "IU001",
    "1": "IU002",
    "2": "IU003",
    "3": "IU004",
    "4": "IU005",
    "5": "IU006",
    "6": "IU007",
    "100": "SPS001",
    "101": "SPS002",
    }

# Map InductionNo with the real world names (unknowns -> "No Map Yet")
message_df["inductionNo"] = message_df["inductionNo"].map(induction_map).fillna("No Map Yet")

# Join dataframes
parsed_df = pd.concat([temp_df.drop(columns=["rawMessage"]), message_df], axis=1)

# Cleaning DataFrame
# Get list of columns with only 1 unique value, but preserve "inductionNo"
cols_to_drop = parsed_df.columns[parsed_df.nunique() == 1].tolist()
if "inductionNo" in cols_to_drop:
    cols_to_drop.remove("inductionNo")
# Usual Columns Dropped
# ['flag', 'systemName', 'ipAddress', 'unkown', 'unkown_2', 'machineCode', 'destinationNo', 'comHost', 'comMode', 'telegramType']    

clean_df = parsed_df.drop(columns=cols_to_drop)
# Usual Columns Remaining
# ['timeStamp', 'sender', 'timeStampPLC', 'messageCode', 'sequenceNo', 'event', 'awcsAction', 'plcRecordNo', 'itemID', 'indexNo', 'awcsStateNow', 'awcsStateNew', 'inductionStatus', 'inductionNo', 'carrierNo', 'carrierCount']

# Rate Analysis
# using time frame window to select a subset of data and preform analysis
global_start_time = clean_df["timeStamp"].min()
global_end_time = clean_df["timeStamp"].max()
global_delta_time = global_end_time - global_start_time

print(f"Start Time: {global_start_time}")
print(f"End Time: {global_end_time}")
print(f"Delta Time: {global_delta_time}\n")

def select_window_cli_24h(df, window_time):
    s = input("Start time 24h (e.g. 16:00 or 16:00:30 or 16): ").strip()
    try:
        t = dt.time.fromisoformat(s)
    except ValueError:
        t = dt.time(int(s), 0, 0)
    start = pd.Timestamp(dt.datetime.combine(global_start_time.date(), t))
    end   = start + pd.Timedelta(minutes=window_time)

    # Check if start is before global start time
    if start < global_start_time:
        print(f"⚠️  WARNING: Requested start time ({start}) is before data begins ({global_start_time})")
        print(f"   → Adjusting start time to data beginning: {global_start_time}")
        start = global_start_time
        end = start + pd.Timedelta(minutes=window_time)
    
    # Check if end exceeds global end time
    if end > global_end_time:
        original_end = end
        end = global_end_time
        actual_window_minutes = (end - start).total_seconds() / 60
        print(f"⚠️  WARNING: Requested end time ({original_end}) exceeds data boundary ({global_end_time})")
        print(f"   → Adjusting end time to data boundary: {global_end_time}")
        print(f"   → Actual window duration: {actual_window_minutes:.1f} minutes (requested: {window_time} minutes)")
    
    # Additional check: if start is also beyond global_end_time
    if start > global_end_time:
        print(f"❌ ERROR: Requested start time ({start}) is after data ends ({global_end_time})")
        print("   → No data available for this time window")
        return df.iloc[0:0].copy(), start, end  # Return empty dataframe

    mask  = (df["timeStamp"] >= start) & (df["timeStamp"] <= end)
    win   = df.loc[mask].copy()
    print(f"\nWindow: {start} → {end}  ({window_time} min) | Rows: {len(win)}")
    return win, start, end

window_df, start_ts, end_ts = select_window_cli_24h(clean_df, WINDOW_TIME)

# Drop "SPS001, SPS002" inductions if any
# This is because we do not have a target PPH for them and its part of another process
window_df = window_df[~window_df["inductionNo"].isin(SPS_NAMES)]

# Get unique induction numbers available in the window data
available_inductions = window_df["inductionNo"].dropna().unique()
available_inductions_sorted = sorted(available_inductions)

print("Available induction numbers in the time window:")
for val in available_inductions_sorted:
    print(f"  - {val}")

def choose_target_pph(inductions_iterable):
    inds = [i for i in inductions_iterable if i in SEMI or i in AUTO]
    u = set(inds)
    if not u:
        return None # no valid inductions found

    if u.issubset(SEMI):
        return TARGET_SINGLE_SEMI_AUTO_PPH if len(u) == 1 else TARGET_ALL_SEMI_AUTO_PPH

    if u.issubset(AUTO):
        return TARGET_SINGLE_AUTO_PPH if len(u) == 1 else TARGET_ALL_AUTO_PPH

    return None  # mixed groups -> no target

target_pph = choose_target_pph(available_inductions)

def analyze_dataset(df, line_name, measuring_point, start_ts, end_ts, target_pph=TARGET_SINGLE_SEMI_AUTO_PPH):
    # Number of items in the window dataframe
    n_items = len(df)
    
    # Time window duration in seconds
    window_secs = (end_ts - start_ts).total_seconds()
    window_min = window_secs / 60.0

    
    # PPH calculation: items in window / time window hr -> items/hr
    if window_secs > 0:
        pph_window = (n_items / window_secs) * 3600.0   # items per hour -> n_items / delta_time to get how many items we did in 1 sec and then multiply by 3600 to get items per hour
        sec_per_bag = window_secs / n_items
        items_per_sec = n_items / window_secs
        items_per_min = items_per_sec * 60
    else:
        pph_window = 0
        sec_per_bag = 0
        items_per_sec = 0
        items_per_min = 0

    # Pass/fail
    needed_items_for_pass = math.ceil((target_pph / 3600) * window_secs)
    items_short = max(0, needed_items_for_pass - n_items)
    passed = (pph_window >= target_pph)

    sec_per_bag_target = 3600 / target_pph

    # Recommendation
    if window_secs > 0 and n_items > 0 and items_short > 0:
        recommendation = (
            f"Need +{items_short} items in this {window_min:.1f} min window "
            f"or reduce sec/bag to ≤ {sec_per_bag_target:.2f} (current {sec_per_bag:.2f})."
        )
    else:
        recommendation = "On target or above. Maintain current rate."

    # Summary line
    meridiem = end_ts.strftime('%p')
    time_label = f"{start_ts:%H%M}-{end_ts:%H%M}{meridiem}"
    summary_line = f"{line_name};Measuring Point: {measuring_point}: Time {time_label}; Total Inducted = {n_items}"

    return {
        "line_name": line_name,
        "measuring_point": measuring_point,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "time_label": time_label,
        "n_items": n_items,
        "window_minutes": window_min,
        "items_per_sec": items_per_sec,
        "items_per_min": items_per_min,
        "pph_window": pph_window,
        "target_pph": target_pph,
        "attainment_window_%": (pph_window / target_pph * 100),
        "needed_items_for_pass": needed_items_for_pass,
        "items_short": items_short,
        "sec_per_bag_current": sec_per_bag,
        "sec_per_bag_target": sec_per_bag_target,
        "recommendation": recommendation,
        "summary_line": summary_line,
        "passed": passed
    }

if target_pph is None:
    print("\n⚠️  WARNING: Mixed induction types detected (both Semi-Auto and Auto). No target PPH defined.")
    print("   → Skipping rate analysis.")
else:
    print(f"\nTarget PPH for this analysis: {target_pph} PPH")
    rows = []
    induction_df = []
    for induction in available_inductions_sorted:
        df_induction = window_df[window_df["inductionNo"] == induction].copy()
        if df_induction.empty:
            continue

        # Use the human-friendly label for line_name; use code if MP expects raw code
        line_name = induction
        measuring_point = "MP1000605"   # <---- What is this?????

        rows.append(analyze_dataset(df_induction, line_name, measuring_point, start_ts, end_ts, target_pph))
        induction_df.append(df_induction.assign(induction=line_name))

    if rows:
        total_items = sum(row["n_items"] for row in rows)
        total_pph = sum(row["pph_window"] for row in rows)
        # count how many inductions contributed to the total
        num_inductions = len(rows)
        individual_pph = target_pph * num_inductions
        overall_attainment = (total_pph / individual_pph * 100)

        rows.append({
            "line_name": "All Inductions",
            "n_items": total_items,
            "pph_window": total_pph,
            "target_pph": individual_pph,
            "attainment_window_%": overall_attainment
        })

        # Export results to Excel
        file_name = f"ratePPH_{start_ts:%Y%m%d-%H%M%S}_{end_ts:%Y%m%d-%H%M%S}.xlsx"
        rate_analysis_df = pd.DataFrame(rows)

        with pd.ExcelWriter(file_name, engine="xlsxwriter") as writer:
            rate_analysis_df.to_excel(writer, sheet_name="rate_analysis", index=False)
            raw_df.to_excel(writer, sheet_name="raw_data", index=False)
            clean_df.to_excel(writer, sheet_name="clean_data", index=False)
            window_df.to_excel(writer, sheet_name="window_data", index=False)

        print("Saved:", file_name)
        print("\nAnalysis Results:")
        for _, row in rate_analysis_df.iterrows():
            print(f"{row['line_name']}: {row['n_items']} items, {row['pph_window']:.1f} PPH "
                  f"({row['attainment_window_%']:.1f}% of target)")
