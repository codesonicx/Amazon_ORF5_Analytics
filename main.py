import pandas as pd
import csv
import datetime as dt
import math

TARGET_PPH = 2450
WINDOW_TIME = 30  # minutes

# ToDo use file picker
path = r"C:\Users\joacosta\Dev\Python\ORF5\Data\VS_A_9-19-2025_ItemInducted_Test-2.csv"

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

temp_df = raw_df.replace('"', '',regex=True)

temp_df.columns = [
    "timeStamp", "flag", "systemName", "ipAddress", "sender", "unkown",
    "unkown_2", "timeStampPLC", "machineCode", "unitID", "sequenceNo",
    "rawMessage"
]

temp_df["rawMessage"] = temp_df["rawMessage"].str.removeprefix("->{").str.removesuffix("}<")

message_df = temp_df["rawMessage"].str.split(",", expand=True)  # Split into columns
col_names = message_df.iloc[0].str.split(":", n=1).str[0].str.strip()   # Extract keys (before ':') from the first row as column names
message_df.columns = col_names  # Assign new column names
message_df = message_df.apply(lambda x: x.str.split(":", n=1).str[1])   # Keep only values (after ':')
clean_df = pd.concat([temp_df.drop(columns=["rawMessage"]), message_df], axis=1)    #join dataframes

# Get list of columns with only 1 unique value
cols_to_drop = clean_df.columns[clean_df.nunique() == 1].tolist()
print("Columns to drop:", cols_to_drop)
df_reduced = clean_df.drop(columns=cols_to_drop)
print("Remaining columns:", df_reduced.columns.tolist())

### Rate Analysis
temp_series = df_reduced["timeStamp"].str.replace(" ", "", regex=True)
df_reduced["timeStamp"] = pd.to_datetime(temp_series, format="%y%m%d%H%M%S%f", errors="coerce")

start_time = df_reduced["timeStamp"].min()
end_time = df_reduced["timeStamp"].max()
delta_time = end_time - start_time

print(f"Start Time: {start_time}")
print(f"End Time: {end_time}")
print(f"Delta Time: {delta_time}")

def select_window_cli_24h(df, window_time):
    s = input("Start time 24h (e.g. 16:00 or 16:00:30 or 16): ").strip()
    try:
        t = dt.time.fromisoformat(s)
    except ValueError:
        t = dt.time(int(s), 0, 0)
    start = pd.Timestamp(dt.datetime.combine(start_time.date(), t))
    end   = start + pd.Timedelta(minutes=window_time)
    mask  = (df["timeStamp"] >= start) & (df["timeStamp"] <= end)
    win   = df.loc[mask].copy()
    print(f"Window: {start} → {end}  ({window_time} min) | Rows: {len(win)}")
    return win, start, end

window_df, start_ts, end_ts = select_window_cli_24h(df_reduced, WINDOW_TIME)

# Normalize inductionNo: remove spaces and convert to integer (nullable)
df_reduced["inductionNo"] = pd.to_numeric(
    df_reduced["inductionNo"].astype(str).str.strip(),
    errors="coerce"
).astype("Int64")

# Group by inductionNo and split
df_IU005 = df_reduced[df_reduced["inductionNo"] == 4].copy()
df_IU006 = df_reduced[df_reduced["inductionNo"] == 5].copy()
df_IU007 = df_reduced[df_reduced["inductionNo"] == 6].copy()

def analyze_dataset(df, line_name, measuring_point, start_ts, end_ts):
    #Data from the general Dataset
    n_items = len(df)
    general_delta_time = df["timeStamp"].max() - df["timeStamp"].min()
    delta_secs = general_delta_time.total_seconds()
    delta_min = delta_secs / 60.0

    # Core metrics
    sec_per_bag   = (delta_secs / n_items)
    items_per_sec = (n_items / delta_secs)
    items_per_min = items_per_sec * 60

    # PPH
    pph_overall = n_items * 3600 / delta_secs
    window_secs = (end_ts - start_ts).total_seconds()
    pph_window = n_items * 3600 / window_secs

    # Pass/fail
    passed = (pph_window >= TARGET_PPH)
    needed_items_for_pass = math.ceil(TARGET_PPH * window_secs / 3600)
    items_short = max(0, needed_items_for_pass - n_items)

    spb_current = window_secs / n_items
    spb_target  = 3600 / TARGET_PPH

    # Recommendation
    if window_secs > 0 and n_items > 0 and items_short > 0:
        recommendation = (
            f"Need +{items_short} items in this {window_secs/60:.1f} min window "
            f"or reduce sec/bag to ≤ {spb_target:.2f} (current {spb_current:.2f})."
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
        "delta_minutes": delta_min,
        "sec_per_bag": sec_per_bag,
        "items_per_sec": items_per_sec,
        "items_per_min": items_per_min,
        "pph_overall": pph_overall,
        "pph_window": pph_window,
        "target_pph": TARGET_PPH,
        "attainment_overall_%": (pph_overall / TARGET_PPH * 100),
        "attainment_window_%": (pph_window / TARGET_PPH * 100),
        "needed_items_for_pass": needed_items_for_pass,
        "items_short": items_short,
        "spb_current": spb_current,
        "spb_target": spb_target,
        "recommendation": recommendation,
        "summary_line": summary_line,
    }

rows = []
rows.append(analyze_dataset(df_IU005, "IU005", "MP1000605", start_ts, end_ts))
rows.append(analyze_dataset(df_IU006, "IU006", "MP1000606", start_ts, end_ts))
rows.append(analyze_dataset(df_IU007, "IU007", "MP1000607", start_ts, end_ts))
rate_analysis_df = pd.DataFrame(rows)

clean_data_df = pd.concat([
    df_IU005.assign(induction="IU005"),
    df_IU006.assign(induction="IU006"),
    df_IU007.assign(induction="IU007"),
], ignore_index=True)

fname = f"ratePPH_{start_ts:%Y%m%d-%H%M%S}_{end_ts:%Y%m%d-%H%M%S}.xlsx"

with pd.ExcelWriter(fname, engine="xlsxwriter") as writer:
    rate_analysis_df.to_excel(writer, sheet_name="rate_analysis", index=False)
    clean_data_df.to_excel(writer, sheet_name="clean_data", index=False)
    raw_df.to_excel(writer, sheet_name="raw_data", index=False)

print("Saved:", fname)



