import ast
import os

import pandas as pd

# Global Constants
from config import (
    DEFECT_CATEGORY_MAP,
    S04_MESSAGE_CODE,
    SORT_CODE_MAP,
    WINDOW_TIME,
)
from utils.data_loader import load_data
from utils.time_frame import select_window_cli


def format_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.replace('"', "", regex=True)  # Remove all double quotes
    df = df.replace(r"\s+", "", regex=True)  # Remove all whitespace

    df.columns = [
        "timeStamp",
        "flag",
        "systemName",
        "ipAddress",
        "sender",
        "unkown",
        "unkown_2",
        "timeStampPLC",
        "mainCabinetName",
        "messageCode",
        "sequenceNo",
        "rawMessage",
    ]

    # Droping records that are not "54177" (S04) in messageCode column
    original_records = len(df)
    df = df[df["messageCode"] == S04_MESSAGE_CODE]
    remaining_records = len(df)
    dropped_count = original_records - remaining_records
    print(
        f"Filtered dataset: kept {remaining_records} rows with messageCode = {S04_MESSAGE_CODE}"
        f"\n\tdropped {dropped_count} out of {original_records} total rows"
    )
    return df


def parse_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    # timeStamp parsing
    df["timeStamp"] = pd.to_datetime(
        df["timeStamp"], format="%y%m%d%H%M%S%f", errors="coerce"
    )

    # Helper functions to handle arrays inside values
    def split_key_values(text):
        """Split key:value pairs by commas, ignoring commas inside [brackets]."""
        parts = []  # Final list of key:value strings
        buf = ""  # Temporary buffer to collect characters
        inside_brackets = 0  # Counter to track nesting depth of [ ]

        for ch in text:
            if ch == "[":
                inside_brackets += 1  # Entering a bracket → increase depth
            elif ch == "]":
                inside_brackets -= 1  # Leaving a bracket → decrease depth

            # Split only on commas that are *outside* brackets
            if ch == "," and inside_brackets == 0:
                parts.append(buf.strip())  # Save the current piece
                buf = ""  # Reset buffer for next piece
            else:
                buf += ch  # Keep building the current piece

        # Append the last piece (after the loop ends)
        if buf:
            parts.append(buf.strip())

        return parts

    def parse_row(text):
        """Convert a rawMessage string into a dictionary of key:value pairs."""
        key_value_strings = split_key_values(
            text
        )  # Split into ["key1:value1", "key2:value2", ...]
        parsed_dict = {}  # Dictionary to hold final result

        for pair in key_value_strings:
            if ":" in pair:  # Only process well-formed pairs
                key, value = pair.split(
                    ":", 1
                )  # Split into key and value (only on the first colon)
                parsed_dict[key.strip()] = value.strip()  # Clean whitespace and store

        return parsed_dict

    def parse_list(val):
        """Convert a string representation of a list into an actual Python list."""
        if isinstance(val, str) and val.startswith("[") and val.endswith("]"):
            try:
                return ast.literal_eval(
                    val
                )  # Safely evaluate the string to a Python list
            except (ValueError, SyntaxError):
                return []  # Return empty list on error
        return [val]  # Fallback: wrap non-list in a list

    def normalize_lists(row, target_cols):
        max_len = max(
            len(row[c]) if isinstance(row[c], list) else 0 for c in target_cols
        )
        for c in target_cols:
            if not isinstance(row[c], list):
                row[c] = [row[c]]
            if len(row[c]) < max_len:
                filler = -1 if c != "requestedDestStatus" else "Unused"
                row[c] = row[c] + [filler] * (max_len - len(row[c]))
        return row

    # Message Column parsing
    df["rawMessage"] = df["rawMessage"].str.removeprefix("->{").str.removesuffix("}<")
    # Expand rawMessage into columns
    # Keeping in mind that some values are lists enclosed in [ ]
    message_df = df["rawMessage"].apply(parse_row).apply(pd.Series)
    # Extract first element from list-like values
    columns_with_arrays = ["requestedDestMCID", "sortCode", "requestedDestStatus"]
    for col in columns_with_arrays:
        message_df[col] = message_df[col].apply(parse_list)

    # Join parsed message columns with the base dataframe
    parsed_df = pd.concat([df.drop(columns=["rawMessage"]), message_df], axis=1)
    interim_df = parsed_df.copy()
    parsed_df = parsed_df.apply(
        normalize_lists, axis=1, target_cols=columns_with_arrays
    )
    parsed_df = parsed_df.explode(column=columns_with_arrays, ignore_index=True)  # type: ignore[arg-type]

    # Drop rows that contain -1 in of the exploded columns
    parsed_df = parsed_df[~parsed_df[columns_with_arrays].isin([-1]).any(axis=1)]

    return parsed_df, interim_df


def drop_constant_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Get list of columns with only 1 unique value, but preserve "sortCode", "indexNo" and "timeStamp"
    # We don't use this method because across multiple sites the way of how they populate these columns may be broken or have inconsistencies
    # cols_to_drop = df.columns[df.nunique() == 1].tolist()
    # So keeping in mind this, we are going to use a hardcoded list of columns to drop
    keep_cols = [
        "timeStamp",
        "plcRecordNo",
        "itemID",
        "indexNo",
        "locationAWCS",
        "barcodeAWCS",
        "actualDestMCID",
        "requestedDestMCID",
        "sortCode",
    ]

    # Usual Columns Remaining
    # ['timeStamp', 'PLCTimeStamp', 'sequenceNo', 'plcRecordNo', 'itemID', 'indexNo', 'locationAWCS', 'barcodeAWCS', 'actualDestMCID', 'requestedDestMCID', 'sortCode']
    return df[keep_cols].copy()


def load_mapping(path: str) -> dict:
    """Load and clean chute mapping file into dictionary."""
    df = load_data(path)
    # Strip quotes/spaces just in case
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x).strip() if pd.notnull(x) else x)

    # Build mapping: IndexNo -> {'amazon': ..., 'beumer': ..., 'jackpot': ...}
    mapping = {
        int(row["IndexNo"]): {
            "amazon": row["Amazon"],
            "beumer": row["Beumer"],
            "jackpot": row["Jackpot"],
        }
        for _, row in df.iterrows()
    }
    return mapping


def enrich_window_df(window_df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Apply all enrichment mappings to window_df."""

    window_df["sortReason"] = window_df["sortCode"].map(SORT_CODE_MAP)
    window_df["defectCategory"] = window_df["sortReason"].map(DEFECT_CATEGORY_MAP)

    map_series = window_df["requestedDestMCID"].map(mapping)
    # Expand mapping into separate columns
    window_df["Amazon_Destination"] = map_series.apply(
        lambda x: x["amazon"] if isinstance(x, dict) else None
    )
    window_df["Beumer_Destination"] = map_series.apply(
        lambda x: x["beumer"] if isinstance(x, dict) else None
    )
    window_df["Jackpot_Destination"] = map_series.apply(
        lambda x: x["jackpot"] if isinstance(x, dict) else None
    )

    return window_df


def remove_false_positives(df: pd.DataFrame) -> pd.DataFrame:
    try:
        print(
            "Please select the Excel file containing the list of indexNo values to remove false positives from."
        )
        bad_ids_df = load_data()
    except ValueError as e:
        print(f"Error loading Excel file: {e}")
        return df

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

    df["No Scan Defect Explanation"] = (
        ""  # Creating New Column where the comments of the user will be stored
    )
    scan_defects = df[df["sortCode"].isin([8, 9, 10])]  # Restrict to scan-defect rows
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
            (scan_defects["indexNo"] == padded_id)
            & (~scan_defects.index.isin(used_rows))
        ]

        if len(candidates) > 0:
            row_idx = candidates[0]  # take the next available one
            df.at[row_idx, "sortCode"] = 0
            df.at[row_idx, "No Scan Defect Explanation"] = (
                comment if pd.notna(comment) else ""
            )
            used_rows.add(row_idx)

            matched_ids.append(padded_id)
            modified_count += 1

    # IDs from user list that didn’t get applied
    not_found = [
        str(int(x)).zfill(4)
        for x in bad_ids_df[id_col]
        if str(int(x)).zfill(4) not in matched_ids
    ]

    print(
        f"Modified sortCode to 0 for {modified_count} rows (respecting duplicates in user list)."
    )
    if matched_ids:
        print("IDs modified:", matched_ids)
    if not_found:
        print("IDs not applied (no scan-defect row left):", not_found)
    return df


def add_package_info(df: pd.DataFrame, threshold_sec: int = 1800) -> pd.DataFrame:
    df = df.copy()
    df["timeStamp"] = pd.to_datetime(df["timeStamp"])

    # Sort and compute package boundaries
    df = df.sort_values(["itemID", "timeStamp"])
    new_pkg_flag = (
        df.groupby("itemID")["timeStamp"]
        .diff()
        .dt.total_seconds()
        .gt(threshold_sec)
        .fillna(True)
    )
    group_idx = new_pkg_flag.groupby(df["itemID"]).cumsum().astype(int)

    # Real package ID
    df["RealPackageID"] = df["itemID"].astype(str) + "_" + group_idx.astype(str)

    # Classify packages
    by_pkg = df.groupby("RealPackageID")
    pkg_all_no_read = by_pkg["barcodeAWCS"].apply(
        lambda s: s.str.fullmatch(r"\?+", na=False).all()
    )
    pkg_all_multi = by_pkg["barcodeAWCS"].apply(
        lambda s: s.str.fullmatch(r"9+", na=False).all()
    )
    pkg_normal = ~(pkg_all_no_read | pkg_all_multi)

    pkg_type = (
        pkg_normal.map({True: "normal", False: None})
        .combine_first(pkg_all_no_read.map({True: "no_read"}))
        .combine_first(pkg_all_multi.map({True: "multi_read"}))
    )

    # Merge back to dataframe
    df = df.merge(
        pkg_type.rename("pkg_type"),
        left_on="RealPackageID",
        right_index=True,
        how="left",
    )

    return df


def scanner_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Counts from pkg_type (already precomputed)
    pkg_type_counts = df.groupby("RealPackageID")["pkg_type"].first().value_counts()

    totalPkgCount = int(pkg_type_counts.sum())
    normalPkgCount = int(pkg_type_counts.get("normal", 0))
    noReadCount = int(pkg_type_counts.get("no_read", 0))
    multiReadCount = int(pkg_type_counts.get("multi_read", 0))

    print("\nScanner Metrics Summary")
    print(f"Total unique packages: {totalPkgCount}")
    print(f"  Normal packages (with real barcodes): {normalPkgCount}")
    print(f"  No Read packages: {noReadCount}")
    print(f"  Multi Read packages: {multiReadCount}")

    return pd.DataFrame(
        {
            "metric": [
                "total_packages",
                "normal_packages",
                "no_read_packages",
                "multi_read_packages",
            ],
            "count": [totalPkgCount, normalPkgCount, noReadCount, multiReadCount],
        }
    )


def sort_code_metrics(df: pd.DataFrame) -> dict:
    df = df.copy()

    # Define recirculation mask (garbage condition)
    recirc_mask = (df["sortCode"] == 0) & (df["requestedDestMCID"].isin([3001, 3002]))

    # Detect recirculation before dropping
    dup_mask = df.duplicated("RealPackageID", keep=False)
    recirculating_ids = df.loc[dup_mask & recirc_mask, "RealPackageID"].unique()
    recirculation_count = len(recirculating_ids)

    # Now drop garbage for analysis
    df = df.loc[~recirc_mask]

    # Deduplicate for per-package metrics
    unique_pkgs = df.drop_duplicates("RealPackageID")

    # Aggregations
    sort_counts = (
        unique_pkgs.groupby("sortReason", as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )

    reason_dest_summary = (
        unique_pkgs.groupby(["sortReason", "Amazon_Destination"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values(["sortReason", "count"], ascending=[True, False])
        .reset_index(drop=True)
    )

    reason_dest_pivot = reason_dest_summary.pivot_table(
        index=["sortReason"], columns="Amazon_Destination", values="count", fill_value=0
    ).reset_index()

    print("\nSort Code Metrics Summary")
    print(sort_counts)

    print("\nSort Reason vs Amazon Destination Breakdown")
    print(reason_dest_summary)

    print("\nRecirculation Packages Count")
    print(f"  Unique packages that recirculated at least once: {recirculation_count}")

    return {
        "sort_counts": sort_counts,
        "reason_dest_summary": reason_dest_summary,
        "reason_dest_pivot": reason_dest_pivot,
        "recirculation_count": recirculation_count,
    }


def defect_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Take only one row per package (first occurrence is enough,
    # since RealPackageID is already classified earlier)
    unique_pkgs = df.drop_duplicates("RealPackageID")
    total_processed = unique_pkgs.shape[0]
    # Count defects only (exclude NaN)
    defect_summary = (
        unique_pkgs["defectCategory"]
        .value_counts(dropna=True)
        .rename_axis("defectCategory")
        .reset_index(name="count")
    )

    # Add "No Defect" row
    defect_count_total = defect_summary["count"].sum()
    no_defect_count = total_processed - defect_count_total

    defect_summary = pd.concat(
        [
            defect_summary,
            pd.DataFrame([{"defectCategory": "No Defect", "count": no_defect_count}]),
        ],
        ignore_index=True,
    )

    # Percent over total processed
    defect_summary["percentage"] = (
        defect_summary["count"] / total_processed * 100
    ).round(4)

    print("\nDefect Metrics Summary")
    print(defect_summary)

    return defect_summary


def jackpot_metrics(df: pd.DataFrame) -> int:
    """
    Calculate how many unique packages have 'Jackpot' destinations
    among successful sort results (sortCode == 0).
    """
    df = df.copy()

    # Filter: success (sortCode == 0) + destination is a jackpot
    mask = (df["sortCode"] == 0) & (
        df["Jackpot_Destination"].astype(str).str.strip().str.lower() == "jackpot"
    )

    # Count unique packages that hit jackpot at least once
    unique_jackpot_packages = df.loc[mask, "RealPackageID"].nunique()

    print("\nJackpot Metrics Summary")
    print(f"  Unique packages that hit jackpot ≥1 time: {unique_jackpot_packages}")

    return unique_jackpot_packages


def export_to_excel(results: dict) -> None:
    os.makedirs("data/reports", exist_ok=True)

    start_str = results["start_ts"].strftime("%Y%m%d-%H%M%S")
    end_str = results["end_ts"].strftime("%Y%m%d-%H%M%S")
    output_path = f"data/reports/Analysis_SO4_{start_str}_{end_str}.xlsx"

    print("\nExporting analysis results to Excel file...")

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        wb = writer.book
        ws = wb.add_worksheet("Analysis_Results")  # type: ignore[attr-defined]
        bold = wb.add_format({"bold": True})  # type: ignore[attr-defined]

        # Analysis Summary
        ws.write("A1", "Analysis Summary", bold)
        ws.write("A2", "Total records (window dataset):")
        ws.write_number("B2", results["S04_processed"])
        ws.write("A3", "Total processed packages:")
        ws.write_number("B3", results["package_processed"])
        ws.write("A4", "Total truly one-off packages:")
        ws.write_number("B4", results["unique_packages"])
        ws.write("A5", "Total recirculation packages:")
        ws.write_number("B5", results["recirculation_packages"])
        ws.write("A6", "Total recirculation records:")
        ws.write_number("B6", results["recirculation_records"])
        ws.write("A7", "Total jackpot packages:")
        ws.write_number("B7", results["jackpot_packages"])

        ws.write("A9", "Time window", bold)
        ws.write("A10", "StartTime:")
        ws.write("B10", results["start_ts"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        ws.write("A11", "EndTime:")
        ws.write("B11", results["end_ts"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])

        # Defect Summary
        start_row_defect = 13
        ws.write("A13", "Defect Category Breakdown", bold)
        results["defect_summary"].to_excel(
            writer,
            sheet_name="Analysis_Results",
            startrow=start_row_defect,
            startcol=0,
            index=False,
        )

        # Sort Reason Counts
        start_row_sort = 19
        ws.write(start_row_sort - 1, 0, "Sort Code Reason Counts", bold)
        results["sort_code_summary"].to_excel(
            writer,
            sheet_name="Analysis_Results",
            startrow=start_row_sort,
            startcol=0,
            index=False,
        )

        # Pivot Table
        start_row_pivot = start_row_sort + len(results["sort_code_summary"]) + 3
        ws.write(start_row_pivot - 1, 0, "SortReason vs RequestedDestMCID", bold)
        results["reason_dest_pivot"].to_excel(
            writer,
            sheet_name="Analysis_Results",
            startrow=start_row_pivot,
            startcol=0,
            index=False,
        )

        # Charts
        # Pie chart for defect breakdown
        chart_pie = wb.add_chart({"type": "pie"})  # type: ignore[attr-defined]
        end_row_def = start_row_defect + len(results["defect_summary"])
        chart_pie.add_series(
            {
                "name": "Defect Category Breakdown",
                "categories": [
                    "Analysis_Results",
                    start_row_defect + 1,
                    0,
                    end_row_def,
                    0,
                ],
                "values": ["Analysis_Results", start_row_defect + 1, 1, end_row_def, 1],
                "data_labels": {
                    "percentage": True,
                    "num_format": "0.0%",
                    "position": "outside_end",
                },
            }
        )
        chart_pie.set_title({"name": "Defect Breakdown"})
        ws.insert_chart(0, 4, chart_pie, {"x_scale": 1.5, "y_scale": 1.5})

        # Column chart for sort counts
        bar_chart = wb.add_chart({"type": "column"})  # type: ignore[attr-defined]
        end_row_sort = start_row_sort + len(results["sort_code_summary"])
        bar_chart.add_series(
            {
                "name": "Sort Code Reason Counts",
                "categories": [
                    "Analysis_Results",
                    start_row_sort + 1,
                    0,
                    end_row_sort,
                    0,
                ],
                "values": ["Analysis_Results", start_row_sort + 1, 1, end_row_sort, 1],
                "data_labels": {"value": True},
            }
        )
        bar_chart.set_title({"name": "Items per Sort Code Reason"})
        ws.insert_chart(0, 16, bar_chart, {"x_scale": 1.5, "y_scale": 1.5})

        # Stacked column chart for pivot table
        end_row_pivot = start_row_pivot + len(results["reason_dest_pivot"])
        end_col_pivot = 20 + len(results["reason_dest_pivot"].columns) - 1
        stack_chart = wb.add_chart({"type": "column", "subtype": "stacked"})  # type: ignore[attr-defined]
        for r in range(start_row_pivot + 1, end_row_pivot + 1):
            stack_chart.add_series(
                {
                    "name": ["Analysis_Results", r, 0],
                    "categories": [
                        "Analysis_Results",
                        start_row_pivot,
                        1,
                        start_row_pivot,
                        end_col_pivot,
                    ],
                    "values": ["Analysis_Results", r, 1, r, end_col_pivot],
                }
            )
        stack_chart.set_title({"name": "Amazon Induction vs SortReason"})
        ws.insert_chart(
            start_row_pivot + len(results["reason_dest_pivot"]) + 1,
            0,
            stack_chart,
            {"x_scale": 8, "y_scale": 3},
        )

        # Extra Sheets
        results["parsed_df"].to_excel(writer, sheet_name="Raw_Data", index=False)
        results["window_df"].to_excel(writer, sheet_name="Window_Data", index=False)
        results["scan_defects"].to_excel(writer, sheet_name="Scan_Defects", index=False)
        results["test_df"].to_excel(writer, sheet_name="Test_Parsed_Data", index=False)

    print(f"Analysis results saved to: {output_path}")


def main():
    print("Select a S04 data file (CSV format) from Log Monitor...")
    try:
        raw_df = load_data()
    except ValueError as e:
        print(e)
        return
    print("Parsing data...")
    format_df = format_data(raw_df)
    parsed_df, interim_df = parse_data(format_df)
    clean_df = drop_constant_cols(parsed_df)

    print("Select time window for analysis:")
    window_df, start_ts, end_ts = select_window_cli(clean_df, WINDOW_TIME)

    print("\nExtracting Mapping Destination Names from Excel...")
    mapping_destination_names = load_mapping(r"data\SAT9_Destination_Mapping.xlsx")

    print("\nEnriching data with mappings...")
    window_df = enrich_window_df(window_df, mapping_destination_names)

    # Optional cleanup of wrong sortCodes
    do_cleanup = (
        input(
            "Do you want to clean up wrong sortCodes using the Excel file? (yes/no): "
        )
        .strip()
        .lower()
    )
    if do_cleanup == "yes":
        print(
            "\nYou selected to remove false positives. Please upload your Excel file containing the indexNo values to remove."
        )
        window_df = remove_false_positives(window_df)
    else:
        print("\nSkipping sortCode cleanup step.\n")

    print("\nGetting analysis metrics...")
    window_df = add_package_info(window_df)
    scanner_df = scanner_metrics(window_df)
    sort_code_results = sort_code_metrics(window_df)
    defect_df = defect_metrics(window_df)
    jackpot_count = jackpot_metrics(window_df)

    analysis_results = {
        # Metadata
        "start_ts": start_ts,
        "end_ts": end_ts,
        "S04_processed": window_df.shape[0],
        "package_processed": window_df["RealPackageID"].nunique(),
        "unique_packages": scanner_df.loc[
            scanner_df["metric"] == "total_packages", "count"
        ].values[0],
        "recirculation_packages": sort_code_results["recirculation_count"],
        "recirculation_records": window_df[
            window_df["RealPackageID"].isin(
                window_df.loc[
                    window_df.duplicated("RealPackageID", keep=False), "RealPackageID"
                ]
            )
        ].shape[0],
        "jackpot_packages": jackpot_count,
        # DataFrames
        "defect_summary": defect_df,
        "sort_code_summary": sort_code_results["sort_counts"],
        "reason_dest_pivot": sort_code_results["reason_dest_pivot"],
        "parsed_df": parsed_df,
        "window_df": window_df,
        "scan_defects": window_df[window_df["sortCode"].isin([8, 9, 10])][
            ["indexNo", "timeStamp", "sortCode"]
        ].copy(),
        "test_df": interim_df,
    }

    export_to_excel(analysis_results)


if __name__ == "__main__":
    main()
