import requests
import csv
from datetime import datetime
import time

# Configuration
API_URL = "https://taskczech-production.up.railway.app/api/data"  # Replace with your server URL
CSV_FILE = "electricity_market_data.csv"  # File to save the data

def fetch_data():
    """
    Fetch data from the FastAPI endpoint.
    """
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

def save_to_csv(data, csv_file):
    """
    Append the fetched data to a CSV file.
    Add a column for the time when the data was fetched.
    """
    # Add a timestamp column
    data["fetch_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Define the CSV column headers
    headers = [
        "interval",
        "traded_volume",
        "purchased_volume",
        "sold_volume",
        "weighted_average_price",
        "min_price",
        "max_price",
        "last_price",
        "fallback_message",
        "last_updated",
        "fetch_time",
    ]

    # Check if the CSV file exists
    file_exists = False
    try:
        with open(csv_file, "r") as f:
            file_exists = True
    except FileNotFoundError:
        pass

    # Append data to the CSV file
    with open(csv_file, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not file_exists:
            writer.writeheader()  # Write headers if the file is new
        writer.writerow(data)

def main():
    """
    Main loop to fetch data every 15 minutes and save it to a CSV file.
    """
    while True:
        print(f"Fetching data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
        data = fetch_data()
        if data:
            print("Data fetched successfully:")
            print(data)
            save_to_csv(data, CSV_FILE)
            print(f"Data saved to {CSV_FILE}.")
        else:
            print("Failed to fetch data.")

        # Wait for 15 minutes before the next fetch
        print("Waiting for 15 minutes before the next fetch...")
        time.sleep(15 * 60)

if __name__ == "__main__":
    main()