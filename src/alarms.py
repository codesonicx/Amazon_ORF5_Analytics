import pandas as pd

from utils.data_loader import load_data, select_file


def load_mapping() -> pd.DataFrame:
    """
    Load and clean the destination mapping file for the specified site.
    Returns a DataFrame with columns: IndexNo, Amazon, Beumer, Jackpot.
    """
    site = input("Enter the site name (e.g., ORF5, SAT9, CNO8): ").strip().upper()
    mapping_path = f"data/{site}_Destination_Mapping.xlsx"

    print(f"\nExtracting Mapping Destination Names for {site}...")

    try:
        # Load Excel file
        df = load_data(mapping_path)

        # Clean strings: remove spaces
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].str.strip()

        print(f"Mapping file for {site} loaded successfully.")
        return df

    except FileNotFoundError:
        print(f"Error: Mapping file for '{site}' not found at '{mapping_path}'.")
    except Exception as e:
        print(f"Error loading mapping for {site}: {e}")

    return pd.DataFrame()


# Read data
path = select_file()
df = pd.read_csv(path)

# Clean Part column
df["Part"] = df["Part"].str.replace("=CBS01.", "", regex=False).str.split("+").str[0]

# Split into destinations and inductions
destinations_df = df[df["Part"].str.contains("CHU")].copy()
inductions_df = df[df["Part"].str.contains("IU")].sort_values("Part")
# Reorder inductions_df columns to match: Part, Message, Duration, Occurrences
inductions_cols = ["Part", "Message", "Duration", "Occurrences"]
inductions_df_2 = inductions_df[inductions_cols].copy()

# Map destination names
mapping_df = load_mapping()
if mapping_df.empty:
    raise ValueError("Mapping loading failed.")

destinations_df = destinations_df.merge(
    mapping_df[["Beumer", "Amazon"]], left_on="Part", right_on="Beumer", how="left"
)

destinations_df["MappingString"] = destinations_df.apply(
    lambda row: f"{row['Beumer']} - {row['Amazon']}"
    if pd.notna(row["Amazon"])
    else row["Beumer"],
    axis=1,
)

# Filter for Jams and clean up
destinations_df = destinations_df[
    destinations_df["Message"].str.contains("Jam", na=False)
]
destinations_df = destinations_df.drop(columns=["Part", "Beumer", "Amazon", "Message"])

# Reorder columns
cols = (
    ["MappingString"]
    + [c for c in destinations_df.columns if c not in ["MappingString", "Duration"]]
    + ["Duration"]
)
destinations_df = destinations_df[cols]

# Add separator row
separator = pd.DataFrame(
    [["---"] * len(destinations_df.columns)], columns=destinations_df.columns
)

# Combine both dataframes
combined_df = pd.concat(
    [destinations_df, separator, inductions_df_2], ignore_index=True
)

# Copy to clipboard
print("Copying results to clipboard...")
combined_df.to_clipboard(index=False)
