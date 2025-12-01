import re
from tkinter import Tk, filedialog

import pandas as pd

# Hide the main tkinter window
root = Tk()
root.withdraw()

# Ask user to select the alarm history CSV file
print("Please select the HistoryAlarms CSV file...")
path = filedialog.askopenfilename(
    title="Select HistoryAlarms CSV file",
    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
)

# Check if user selected a file
if not path:
    print("No file selected. Exiting...")
    exit()

# Load the alarm history CSV file
# skiprows=1 skips the first row (usually a header we don't need)
df = pd.read_csv(path, sep=";", skiprows=1)


# Function to extract chute name from the "Part name" column
# Example: "Something CHU731 Something" becomes "CHU731"
def extract_chute(part_name):
    match = re.search(r"(CHU\d+)", part_name)
    return match.group(1) if match else None


df["Chute"] = df["Part name"].apply(extract_chute)


# Function to clean up the duration text
# Removes extra characters like =" and "
def clean_duration(x):
    if isinstance(x, str):
        x = x.replace('="', "").replace('"', "")
    return x


# Clean and convert duration to a time format pandas can use
df["Duration"] = df["Duration"].apply(clean_duration)
df["Duration"] = pd.to_timedelta(df["Duration"])


# Function to load the mapping file that connects Beumer chute names to Amazon names
def load_chute_mapping():
    # Ask user to select the mapping Excel file
    print("Please select the MTN6 Destination Mapping Excel file...")
    mapping_path = filedialog.askopenfilename(
        title="Select MTN6 Destination Mapping Excel file",
        filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
    )

    # Check if user selected a file
    if not mapping_path:
        print("No mapping file selected. Exiting...")
        exit()

    # Load the Excel file
    map_df = pd.read_excel(mapping_path, dtype=str)

    # Remove extra spaces from all text
    for col in map_df.columns:
        map_df[col] = map_df[col].str.strip()

    # Return only the two columns we need
    return map_df[["Beumer", "Amazon"]]


# Load the mapping and add Amazon names to our data
mapping_df = load_chute_mapping()
df = df.merge(mapping_df, left_on="Chute", right_on="Beumer", how="left")


# Group all alarms by chute and calculate statistics
grouped = df.groupby("Chute").agg(
    jam_count=("Chute", "count"),  # Count how many jams per chute
    total_duration=("Duration", "sum"),  # Add up all jam durations
    average_duration=("Duration", "mean"),  # Calculate average jam duration
    max_duration=("Duration", "max"),  # Find the longest jam
    amazon_name=("Amazon", "first"),  # Get the Amazon name for this chute
)

# Sort chutes by total duration (worst offenders first)
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
        "Chute": "Beumer",
        "amazon_name": "Amazon",
        "jam_count": "Jam Count",
        "total_duration": "Total Duration",
        "average_duration": "Average Duration",
        "max_duration": "Max Duration",
    }
)

# Put columns in a logical order
grouped = grouped[
    [
        "Beumer",
        "Amazon",
        "Jam Count",
        "Total Duration",
        "Average Duration",
        "Max Duration",
    ]
]

# Copy results to clipboard so you can paste into Excel
grouped.to_clipboard(index=False)

print("Done! Results copied to clipboard. You can now paste into Excel.")
