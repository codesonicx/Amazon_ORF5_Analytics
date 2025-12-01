import re
from tkinter import Tk, filedialog

import pandas as pd

# Hide the main tkinter window
root = Tk()
root.withdraw()

# Ask user to select the alarm history CSV file
print("Please select the IAS (Item Alignment Sensor) alarm CSV file...")
path = filedialog.askopenfilename(
    title="Select IAS Alarm CSV file",
    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
)

# Check if user selected a file
if not path:
    print("No file selected. Exiting...")
    exit()

# Load the alarm history CSV file
# The format has a header row with filter info, skip it
df = pd.read_csv(path, sep=";", skiprows=1)


# Function to extract IAS identifier from the "Part name" column
# Example: " =CBS01.IAS002+S001-U01" becomes "IAS002"
def extract_ias(part_name):
    if pd.isna(part_name):
        return None
    match = re.search(r"(IAS\d+)", str(part_name))
    return match.group(1) if match else None


df["IAS"] = df["Part name"].apply(extract_ias)


# Function to clean up the duration text
# Removes extra characters like =" and "
def clean_duration(x):
    if isinstance(x, str):
        x = x.replace('="', "").replace('"', "")
    return x


# Clean and convert duration to a time format pandas can use
df["Duration"] = df["Duration"].apply(clean_duration)
df["Duration"] = pd.to_timedelta(df["Duration"])


# Group all alarms by IAS and calculate statistics
grouped = df.groupby("IAS").agg(
    alarm_count=("IAS", "count"),  # Count how many alarms per IAS
    total_duration=("Duration", "sum"),  # Add up all alarm durations
    average_duration=("Duration", "mean"),  # Calculate average alarm duration
    max_duration=("Duration", "max"),  # Find the longest alarm
)

# Sort IAS by total duration (worst offenders first)
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
        "IAS": "Item Alignment Sensor",
        "alarm_count": "Alarm Count",
        "total_duration": "Total Duration",
        "average_duration": "Average Duration",
        "max_duration": "Max Duration",
    }
)

# Put columns in a logical order
grouped = grouped[
    [
        "Item Alignment Sensor",
        "Alarm Count",
        "Total Duration",
        "Average Duration",
        "Max Duration",
    ]
]

# Copy results to clipboard so you can paste into Excel
grouped.to_clipboard(index=False)

print("\nItem Alignment Sensor (IAS) Summary:")
print("=" * 70)
print(grouped.to_string(index=False))
print("=" * 70)
print("\nDone! Results copied to clipboard. You can now paste into Excel.")
