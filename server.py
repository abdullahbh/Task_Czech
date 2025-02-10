from fastapi import FastAPI, HTTPException
import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import pytz
import time
import threading
from bs4 import BeautifulSoup

app = FastAPI()

# Global variables to store the latest data and last fetched interval
latest_data = None
last_fetched_interval = None

def fetch_and_process_data():
    """
    Fetch the OTE website Excel file, parse it into a DataFrame,
    clean columns, etc.
    """
    try:
        url = "https://www.ote-cr.cz/cs/kratkodobe-trhy/elektrina/vnitrodenni-trh"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        container = soup.find("p", class_="report_attachment_links")
        if not container:
            raise ValueError("Failed to find the report attachment container.")

        link_tag = container.find("a")
        if not link_tag or not link_tag.get("href"):
            raise ValueError("Failed to find the download link.")

        file_href = link_tag["href"]
        file_link = "https://www.ote-cr.cz" + file_href
        file_response = requests.get(file_link, timeout=10)
        file_response.raise_for_status()

        excel_file = BytesIO(file_response.content)
        df = pd.read_excel(excel_file, header=None)
        if df.empty:
            raise ValueError("Downloaded file is empty.")

        # Use row 6 (index 5) as headers
        df.columns = df.iloc[5]
        df = df[6:].reset_index(drop=True)

        # Clean column names
        df.columns = (
            df.columns
            .str.strip()
            .str.replace("\n", "", regex=True)
            .str.replace(" +", " ", regex=True)
        )

        # Drop rows that are fully empty
        df = df.dropna(how="all")

        required_cols = [
            "Časový interval",
            "Zobchodované množství(MWh)",
            "Zobchodované množství - nákup(MWh)",
            "Zobchodované množství - prodej(MWh)",
            "Vážený průměr cen (EUR/MWh)",
            "Minimální cena(EUR/MWh)",
            "Maximální cena(EUR/MWh)",
            "Poslední cena(EUR/MWh)",
        ]
        for c in required_cols:
            if c not in df.columns:
                raise ValueError(f"Missing column '{c}' in DataFrame.")

        # Convert the interval column to string and strip
        df["Časový interval"] = df["Časový interval"].astype(str).str.strip()

        return df

    except Exception as e:
        print(f"Error while fetching/processing data: {e}")
        return None

def get_current_time_block(df):
    """
    Find the row whose time interval covers the current CET time.
    If none, pick the last interval that started before now.
    If that row is empty, fallback to older data.
    """
    cet_tz = pytz.timezone("Europe/Prague")
    now = datetime.now(cet_tz).time()

    valid_rows = []
    for idx, row in df.iterrows():
        interval_str = row["Časový interval"]

        # Skip repeated headers or invalid lines
        if "Perioda" in interval_str or "Časový interval" in interval_str:
            continue

        try:
            start_str, end_str = interval_str.split("-")
            st = datetime.strptime(start_str.strip(), "%H:%M").time()
            et = datetime.strptime(end_str.strip(), "%H:%M").time()
        except ValueError:
            # skip un-parseable intervals
            continue

        crosses_midnight = (st > et)
        valid_rows.append((idx, st, et, crosses_midnight))

    if not valid_rows:
        if len(df) > 0:
            return df.iloc[-1], "No parseable intervals found; showing last row by default."
        else:
            return None, "No data at all."

    matching_idx = None
    # We'll store (idx, start_time) so we can compare time objects
    last_before = None

    for (idx, st, et, crosses_midnight) in valid_rows:
        if crosses_midnight:
            # e.g., 23:45-00:00
            if now >= st or now < et:
                matching_idx = idx
                break
        else:
            if st <= now < et:
                matching_idx = idx
                break

        # Track the last interval that started before now
        if st <= now:
            if last_before is None:
                last_before = (idx, st)
            else:
                if st > last_before[1]:
                    last_before = (idx, st)

    if matching_idx is not None:
        row = df.iloc[matching_idx]
        if row_is_empty(row):
            return get_fallback_row(df, matching_idx)
        else:
            return row, ""
    else:
        # No exact match
        if last_before is not None:
            row = df.iloc[last_before[0]]
            if row_is_empty(row):
                return get_fallback_row(df, last_before[0])
            else:
                msg = (f"No exact match for current time. "
                       f"Showing last known data from {row['Časový interval']}.")
                return row, msg
        else:
            # All intervals start after now
            first_idx = valid_rows[0][0]
            row = df.iloc[first_idx]
            if row_is_empty(row):
                return get_fallback_row(df, first_idx)
            msg = (f"All intervals start after {now.strftime('%H:%M')}. "
                   f"Showing earliest interval in data: {row['Časový interval']}.")
            return row, msg

def row_is_empty(row):
    """
    Check if all numeric columns are NaN or blank, meaning no real data.
    Also check if any column contains '-' (indicating incomplete data).
    """
    num_cols = [
        "Zobchodované množství(MWh)",
        "Zobchodované množství - nákup(MWh)",
        "Zobchodované množství - prodej(MWh)",
        "Vážený průměr cen (EUR/MWh)",
        "Minimální cena(EUR/MWh)",
        "Maximální cena(EUR/MWh)",
        "Poslední cena(EUR/MWh)",
    ]
    for c in num_cols:
        val = row.get(c)
        if pd.isna(val) or str(val).strip() == "" or str(val).strip() == "-":
            return True
    return False

def get_fallback_row(df, start_idx):
    """
    Walk backward from start_idx until we find a non-empty row.
    """
    for i in range(start_idx, -1, -1):
        row = df.iloc[i]
        if not row_is_empty(row):
            msg = (f"No new data available after interval {row['Časový interval']}. "
                   f"Showing last known data from {row['Časový interval']}.")
            return row, msg

    if len(df) > 0:
        return df.iloc[0], "No non-empty row found; showing the earliest row."
    else:
        return None, "No data in DataFrame at all."

def next_quarter_hour(now):
    """
    Returns a new datetime rounded up to the next quarter hour: xx:00, xx:15, xx:30, xx:45.
    """
    minute = (now.minute // 15 + 1) * 15
    if minute == 60:
        minute = 0
        hour = now.hour + 1
        if hour == 24:
            hour = 0
            now += timedelta(days=1)
    else:
        hour = now.hour
    return now.replace(hour=hour, minute=minute, second=0, microsecond=0)

def update_data():
    """
    Fetch and update the latest data immediately, then every 15 minutes.
    Retry until new data is available or max retries are reached.
    """
    global latest_data, last_fetched_interval
    cet_tz = pytz.timezone("Europe/Prague")
    now = datetime.now(cet_tz)

    # Fetch data immediately
    fetch_data()

    # Calculate the next quarter-hour mark
    next_run = next_quarter_hour(now)
    wait_seconds = (next_run - now).total_seconds()
    print(f"Waiting until {next_run.strftime('%H:%M:%S')} for the next fetch...")
    time.sleep(wait_seconds)

    while True:
        fetch_data()
        # Wait for 15 minutes before the next fetch
        next_run += timedelta(minutes=15)
        wait_seconds = (next_run - datetime.now(cet_tz)).total_seconds()
        if wait_seconds > 0:
            print(f"Waiting until {next_run.strftime('%H:%M:%S')} for the next fetch...")
            time.sleep(wait_seconds)

def fetch_data():
    """
    Fetch and process data, then update the global variables.
    Retry if data contains '-' or is incomplete.
    """
    global latest_data, last_fetched_interval
    max_retries = 10  # Maximum number of retries
    retry_delay = 60  # Wait 1 minute between retries

    for attempt in range(max_retries):
        df = fetch_and_process_data()
        if df is not None and not df.empty:
            row, fallback_msg = get_current_time_block(df)
            current_interval = row.get("Časový interval", "NA")

            # Check if the data contains '-' (incomplete data)
            if not row_is_empty(row):
                latest_data = {
                    "interval": current_interval,
                    "traded_volume": row.get("Zobchodované množství(MWh)", "NA"),
                    "purchased_volume": row.get("Zobchodované množství - nákup(MWh)", "NA"),
                    "sold_volume": row.get("Zobchodované množství - prodej(MWh)", "NA"),
                    "weighted_average_price": row.get("Vážený průměr cen (EUR/MWh)", "NA"),
                    "min_price": row.get("Minimální cena(EUR/MWh)", "NA"),
                    "max_price": row.get("Maximální cena(EUR/MWh)", "NA"),
                    "last_price": row.get("Poslední cena(EUR/MWh)", "NA"),
                    "fallback_message": fallback_msg,
                    "last_updated": datetime.now(pytz.timezone("Europe/Prague")).strftime("%Y-%m-%d %H:%M:%S")
                }
                last_fetched_interval = current_interval
                print("New data fetched successfully.")
                break
            else:
                print(f"Data contains '-' or is incomplete. Retrying in {retry_delay} seconds...")
        else:
            print(f"Failed to fetch data. Retrying in {retry_delay} seconds...")

        time.sleep(retry_delay)

@app.get("/api/data")
def get_latest_data():
    if latest_data is None:
        raise HTTPException(status_code=503, detail="No data available yet")
    return latest_data

if __name__ == "__main__":
    # Start the data update loop in a separate thread
    threading.Thread(target=update_data, daemon=True).start()
    # Run the FastAPI app
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)