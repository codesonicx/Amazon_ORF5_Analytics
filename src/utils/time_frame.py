import datetime as dt
import pandas as pd

def retrieve_global_time_bounds(df):
    """
    Retrieve the global start and end timestamps from the dataframe.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataset containing a 'timeStamp' column.

    Returns
    -------
    global_start_time : pd.Timestamp
        Earliest timestamp in dataset.
    global_end_time : pd.Timestamp
        Latest timestamp in dataset.
    """
    if "timeStamp" not in df.columns:
        raise ValueError("DataFrame must contain a 'timeStamp' column")
    
    if df["timeStamp"].isnull().all():
        raise ValueError("All values in 'timeStamp' column are NaT")
    
    global_start_time = df["timeStamp"].min()
    global_end_time = df["timeStamp"].max()

    print(f"Start Time: {global_start_time}")
    print(f"End Time: {global_end_time}")
    print(f"Delta Time: {global_end_time - global_start_time}\n")

    return global_start_time, global_end_time

def parse_datetime_or_time(s, default_date):
    """
    Parse a string into a pandas.Timestamp.

    Supports:
    - Time only (e.g. "16:00" → combined with default_date)
    - Hour only (e.g. "16" → 16:00:00 on default_date)
    - Full datetime (e.g. "2025-09-24 16:00")
    """
    try:
        # Try time-only input like "HH:MM" or "HH:MM:SS"
        t = dt.time.fromisoformat(s)
        return pd.Timestamp(dt.datetime.combine(default_date.date(), t))
    except ValueError:
        pass
    
    try:
        # Try simple hour like '16'
        t = dt.time(int(s), 0, 0)
        return pd.Timestamp(dt.datetime.combine(default_date.date(), t))
    except (ValueError, TypeError):
        pass
    
    try:
        # Try full datetime string like "YYYY-MM-DD HH:MM:SS"
        return pd.Timestamp(s)
    except ValueError:
        raise ValueError(f"Could not parse '{s}' as time or datetime")


def select_window_cli(df, window_time):
    """
    Prompt the user to select a start and end time window for analysis.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataset containing a 'timeStamp' column.
    window_time : int
        Default duration (in minutes) if no end time is provided.

    Returns
    -------
    win : pandas.DataFrame
        Subset of df within the selected time window.
    """
    global_start_time, global_end_time = retrieve_global_time_bounds(df)

    choice = input("Type 'Full' to scan the entire dataset, or press Enter to define a time window: ").strip().lower()
    if choice == "full":
        print(f"\n⚡ Using the full dataset: {global_start_time} → {global_end_time} | Rows: {len(df)}")
        return df.copy(), global_start_time, global_end_time

    # Build example inputs for the user prompt
    start_example_full = global_start_time.strftime("%Y-%m-%d %H:%M")
    end_example_full   = global_end_time.strftime("%Y-%m-%d %H:%M")
    start_example_time = global_start_time.strftime("%H:%M")
    end_example_time   = global_end_time.strftime("%H:%M")

    # Get start time
    s = input(
        f"Start → Example: '{start_example_time}' (24h format) "
        f"or '{start_example_full}' (full datetime), "
        f"or press Enter to use dataset start: "
    ).strip()
    
    if s:
        start = parse_datetime_or_time(s, global_start_time)
    else:
        start = global_start_time

    if start < global_start_time:
        print(f"⚠️  WARNING: Requested start time ({start}) is before data begins ({global_start_time})")
        print(f"   → Adjusting start time to data beginning: {global_start_time}")
        start = global_start_time
    
    if start > global_end_time:
        print(f"❌ ERROR: Requested start time ({start}) is after data ends ({global_end_time})")
        print("   → No data available for this time window")
        return df.iloc[0:0].copy(), start, start

    # Get end time
    e = input(
        f"End   → Example: '{end_example_time}' (24h format) "
        f"or '{end_example_full}' (full datetime), "
        f"or press Enter to use {window_time} min window: "
    ).strip()
    
    if e:
        end = parse_datetime_or_time(e, start)
    else:
        end = start + pd.Timedelta(minutes=window_time)

    if end > global_end_time:
        original_end = end
        end = global_end_time
        actual_window_minutes = (end - start).total_seconds() / 60
        print(f"⚠️  WARNING: Requested end time ({original_end}) exceeds data boundary ({global_end_time})")
        print(f"   → Adjusting end time to data boundary: {global_end_time}")
        print(f"   → Actual window duration: {actual_window_minutes:.1f} minutes (requested: {window_time} minutes)")

    if end < start:
        print(f"❌ ERROR: End time ({end}) is before start time ({start})")
        print("   → No valid time window")
        return df.iloc[0:0].copy(), start, end

    mask = (df["timeStamp"] >= start) & (df["timeStamp"] <= end)
    win = df.loc[mask].copy()
    actual_duration = (end - start).total_seconds() / 60
    print(f"\nWindow: {start} → {end}  ({actual_duration:.1f} min) | Rows: {len(win)}")
    return win, start, end
