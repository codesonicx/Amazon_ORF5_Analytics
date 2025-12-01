import re
from tkinter import Tk, filedialog

import pandas as pd

# Hide the main tkinter window
root = Tk()
root.withdraw()

# Ask user to select the alarm history CSV file
print("Please select the DBS alarm CSV file...")
path = filedialog.askopenfilename(
    title="Select DBS Alarm CSV file",
    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
)

# Check if user selected a file
if not path:
    print("No file selected. Exiting...")
    exit()

# Load the alarm history CSV file
# The new format has a header row with filter info, skip it
df = pd.read_csv(path, sep=";", skiprows=1)


# Function to extract DBS identifier from the "Part name" column
# Example: " =CBS01.DBS008+S001-U01" becomes "DBS008"
def extract_dbs(part_name):
    if pd.isna(part_name):
        return None
    match = re.search(r"(DBS\d+)", str(part_name))
    return match.group(1) if match else None


df["DBS"] = df["Part name"].apply(extract_dbs)


# Function to clean up the duration text
# Removes extra characters like =" and "
def clean_duration(x):
    if isinstance(x, str):
        x = x.replace('="', "").replace('"', "")
    return x


# Clean and convert duration to a time format pandas can use
df["Duration"] = df["Duration"].apply(clean_duration)
df["Duration"] = pd.to_timedelta(df["Duration"])


# Group all alarms by DBS and calculate statistics
grouped = df.groupby("DBS").agg(
    alarm_count=("DBS", "count"),  # Count how many alarms per DBS
    total_duration=("Duration", "sum"),  # Add up all alarm durations
    average_duration=("Duration", "mean"),  # Calculate average alarm duration
    max_duration=("Duration", "max"),  # Find the longest alarm
)

# Sort DBS by total duration (worst offenders first)
grouped = grouped.sort_values(by="total_duration", ascending=False)


# Function to convert time to readable HH:MM:SS format
def format_hhmmss(td):
    if pd.isna(td):
        return ""

    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


# Convert all duration columns to HH:MM:SS format
for col in ["total_duration", "average_duration", "max_duration"]:
    grouped[col] = grouped[col].apply(format_hhmmss)


# Clean up the final table with better column names
grouped = grouped.reset_index().rename(
    columns={
        "DBS": "DBS Unit",
        "alarm_count": "Alarm Count",
        "total_duration": "Total Duration",
        "average_duration": "Average Duration",
        "max_duration": "Max Duration",
    }
)

# Put columns in a logical order
grouped = grouped[
    [
        "DBS Unit",
        "Alarm Count",
        "Total Duration",
        "Average Duration",
        "Max Duration",
    ]
]

# Copy results to clipboard so you can paste into Excel
grouped.to_clipboard(index=False)

print("\nDBS Alarm Summary:")
print("=" * 70)
print(grouped.to_string(index=False))
print("=" * 70)
print("\nDone! Results copied to clipboard. You can now paste into Excel.")
