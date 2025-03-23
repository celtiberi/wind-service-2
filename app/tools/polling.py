import requests
from bs4 import BeautifulSoup
import os
import time
import json
from datetime import datetime, timedelta
import threading
import signal
import sys

# Base URL for GFS prod directory
prod_url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"

# Headers to mimic a browser request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Local directory to save downloaded files
download_dir = "gfs_atmos_p25"
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

# File to store polling state
state_file = "polling.json"

# Expected forecast hours (0 to 384, every 3 hours)
forecast_hours = list(range(0, 385, 3))  # [0, 3, 6, ..., 384]

# Polling interval (seconds) and timeout (48 hours to handle date rollover)
poll_interval = 300  # 5 minutes
timeout_hours = 48
timeout = datetime.now() + timedelta(hours=timeout_hours)

# Global stop event for graceful shutdown
stop_event = threading.Event()

# Global flag for downloading all files
download_all_files = False

def quit(signo, _frame):
    """Handle shutdown signals"""
    print(f"\nInterrupted by signal {signo}, shutting down...")
    stop_event.set()

def set_download_all_files(value: bool):
    """Set whether to download all files or just f000"""
    global download_all_files
    download_all_files = value

def stop_polling():
    """Signal the polling thread to stop"""
    stop_event.set()

def load_state():
    """Load polling state from file"""
    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
            return (
                state.get('latest_date'),
                state.get('latest_cycle'),
                set(state.get('downloaded_files', [])),
                set(state.get('current_cycle_files', [])),
                state.get('is_downloading', False),
                state.get('last_update')
            )
    except FileNotFoundError:
        return None, None, set(), set(), False, None

def save_state(latest_date, latest_cycle, downloaded_files, current_cycle_files, is_downloading=False):
    """Save polling state to file"""
    state = {
        'latest_date': latest_date,
        'latest_cycle': latest_cycle,
        'downloaded_files': sorted(list(downloaded_files)),
        'current_cycle_files': sorted(list(current_cycle_files)),
        'is_downloading': is_downloading,
        'last_update': datetime.now().isoformat()
    }
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)

def poll_gfs_data():
    """
    Poll for GFS data files and download them when available.
    This function runs indefinitely until stop_event is set.
    """
    # Initialize state
    latest_date, latest_cycle, downloaded_files, current_cycle_files, is_downloading, last_update = load_state()
    if not latest_date:
        print(f"No previous state found in {state_file}. Starting fresh.")
    else:
        print(f"Resumed state from {state_file}:")
        print(f"Latest date: {latest_date}, Latest cycle: {latest_cycle}")
        print(f"Downloaded {len(downloaded_files)} files, tracking {len(current_cycle_files)} files for current cycle")

    # If resuming, reconstruct base_url and cycle_time
    if latest_date and latest_cycle:
        cycle_time = f"t{latest_cycle}z"
        base_url = f"{prod_url}{latest_date}/{latest_cycle}/atmos/"
        if not current_cycle_files:
            # Only track f000 file if not downloading all files
            current_cycle_files = {f"gfs.{cycle_time}.pgrb2.0p25.f000"} if not download_all_files else {f"gfs.{cycle_time}.pgrb2.0p25.f{h:03d}" for h in forecast_hours}
        print(f"Resumed with base_url: {base_url}")
    else:
        base_url = None
        cycle_time = None

    print(f"Starting polling for GFS p25 files at {prod_url}")
    print(f"Polling every {poll_interval} seconds, timeout after {timeout_hours} hours")
    print(f"Download mode: {'All files' if download_all_files else 'Only f000 files'}")

    while not stop_event.is_set():
        try:
            # Update state to indicate we're downloading
            save_state(latest_date, latest_cycle, downloaded_files, current_cycle_files, is_downloading=True)

            # Step 1: Determine the latest date directory
            response = requests.get(prod_url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all date directories (e.g., gfs.20250322/, gfs.20250323/)
            links = soup.find_all('a')
            date_dirs = [link.text.strip('/') for link in links if link.text.startswith('gfs.')]
            if not date_dirs:
                print("No date directories found. Retrying after delay...")
                if stop_event.wait(poll_interval):
                    break
                continue

            # Get the latest date (e.g., gfs.20250323 > gfs.20250322)
            new_date = max(date_dirs, key=lambda d: datetime.strptime(d, 'gfs.%Y%m%d'))
            if new_date != latest_date:
                latest_date = new_date
                print(f"\nNew date directory detected: {latest_date}")
                # Reset cycle to force recheck
                latest_cycle = None
                save_state(latest_date, latest_cycle, downloaded_files, current_cycle_files, is_downloading=True)

            # Step 2: Check for the latest forecast cycle in the latest date directory
            date_url = f"{prod_url}{latest_date}/"
            response = requests.get(date_url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all cycle directories (e.g., 00/, 06/, 12/, 18/)
            links = soup.find_all('a')
            cycles = [link.text.strip('/') for link in links if link.text.strip('/').isdigit()]
            if not cycles:
                print(f"No forecast cycles found in {latest_date}. Retrying after delay...")
                if stop_event.wait(poll_interval):
                    break
                continue

            # Determine the latest cycle
            new_cycle = max(cycles, key=int)  # e.g., '18' > '12'
            if new_cycle != latest_cycle:
                # New cycle detected, reset the download list
                latest_cycle = new_cycle
                cycle_time = f"t{latest_cycle}z"                
                base_url = f"{date_url}/{latest_cycle}/atmos/"
                # Only track f000 file if not downloading all files
                current_cycle_files = {f"gfs.{cycle_time}.pgrb2.0p25.f000"} if not download_all_files else {f"gfs.{cycle_time}.pgrb2.0p25.f{h:03d}" for h in forecast_hours}
                downloaded_files = {f for f in downloaded_files if not f.startswith(f"gfs.{cycle_time}")}
                print(f"\nNew forecast cycle detected: {latest_date}/{latest_cycle}Z")
                print(f"Base URL updated to: {base_url}")
                print(f"Expected files for this cycle: {len(current_cycle_files)} ({'f000 only' if not download_all_files else 'f000 to f384'})")
                save_state(latest_date, latest_cycle, downloaded_files, current_cycle_files, is_downloading=True)

            # Step 3: Check for available files in the latest cycle
            try:
                print(f"Attempting to access: {base_url}")
                response = requests.get(base_url, headers=headers)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                if response.status_code == 403:
                    print(f"403 Forbidden error accessing {base_url}. This cycle may not be fully available yet.")
                    print("Retrying after delay...")
                    if stop_event.wait(poll_interval):
                        break
                    continue
                raise

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all p25 files in the atmos/ directory
            links = soup.find_all('a')
            available_files = {link.text for link in links if link.text.startswith(f"gfs.{cycle_time}.pgrb2.0p25.f")}
            print(f"Available files: {len(available_files)} found")

            # Download new files
            new_files = available_files - downloaded_files
            print(f"New files to download: {len(new_files)}")
            for file_name in sorted(new_files):
                if stop_event.is_set():
                    break
                if file_name in current_cycle_files:
                    print(f"Found new file: {file_name}")
                    file_url = base_url + file_name
                    local_path = os.path.join(download_dir, file_name)

                    # Download the file
                    print(f"Downloading {file_url} to {local_path}")
                    file_response = requests.get(file_url, headers=headers, stream=True)
                    file_response.raise_for_status()
                    with open(local_path, 'wb') as f:
                        for chunk in file_response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    print(f"Downloaded {file_name}")
                    downloaded_files.add(file_name)
                    save_state(latest_date, latest_cycle, downloaded_files, current_cycle_files, is_downloading=True)

            # Step 4: Check if all expected files for the current cycle are downloaded
            if current_cycle_files.issubset(downloaded_files):
                # Check if the current cycle is 18Z (last of the day)
                if latest_cycle == '18':
                    print(f"All files for {latest_date}/{latest_cycle}Z downloaded. Waiting for next day's 00Z cycle...")
                    # Wait for the next day's 00Z cycle
                    current_utc = datetime.utcnow()
                    next_day = (current_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                    wait_seconds = (next_day - current_utc).total_seconds()
                    if wait_seconds > 0:
                        print(f"Waiting {wait_seconds/60:.1f} minutes for next day's 00Z cycle...")
                        if stop_event.wait(int(wait_seconds)):
                            break
                    latest_date = None  # Force recheck of date
                    latest_cycle = None
                    save_state(latest_date, latest_cycle, downloaded_files, current_cycle_files, is_downloading=True)
                    continue
                else:
                    print(f"All expected files for {latest_date}/{latest_cycle}Z have been downloaded!")
                    # Update state to indicate we're not currently downloading
                    save_state(latest_date, latest_cycle, downloaded_files, current_cycle_files, is_downloading=False)
                    break

            # Check for timeout
            if datetime.now() > timeout:
                print("Timeout reached. Stopping polling.")
                print(f"Downloaded {len(downloaded_files.intersection(current_cycle_files))} out of {len(current_cycle_files)} expected files for cycle {latest_date}/{latest_cycle}Z.")
                # Update state to indicate we're not currently downloading
                save_state(latest_date, latest_cycle, downloaded_files, current_cycle_files, is_downloading=False)
                break

            # Wait before the next poll
            print(f"Waiting {poll_interval} seconds before next poll... "
                  f"({len(downloaded_files.intersection(current_cycle_files))}/{len(current_cycle_files)} files downloaded for cycle {latest_date}/{latest_cycle}Z)")
            if stop_event.wait(poll_interval):
                break

        except requests.RequestException as e:
            print(f"Error during polling: {e}")
            print("Retrying after delay...")
            if stop_event.wait(poll_interval):
                break
        except Exception as e:
            print(f"Unexpected error: {e}")
            # Update state to indicate we're not currently downloading
            save_state(latest_date, latest_cycle, downloaded_files, current_cycle_files, is_downloading=False)
            break

    # Final summary
    print(f"\nPolling complete. Downloaded {len(new_files)} new file(s):")
    for file_name in sorted(new_files):
        print(file_name)
    missing_files = current_cycle_files - downloaded_files
    if missing_files:
        print(f"\nMissing files for cycle {latest_date}/{latest_cycle}Z ({len(missing_files)}):")
        for file_name in sorted(missing_files):
            print(file_name)
    
    # Save final state
    save_state(latest_date, latest_cycle, downloaded_files, current_cycle_files, is_downloading=False)

def start_polling():
    """Start the GFS polling in a separate thread"""
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)
    signal.signal(signal.SIGHUP, quit)
    
    polling_thread = threading.Thread(target=poll_gfs_data, daemon=True)
    polling_thread.start()
    return polling_thread

if __name__ == "__main__":
    # If run directly, start polling in the main thread
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)
    signal.signal(signal.SIGHUP, quit)
    poll_gfs_data()