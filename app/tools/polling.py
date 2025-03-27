import requests
from bs4 import BeautifulSoup
import os
import time
import json
from datetime import datetime, timedelta
import threading
import signal
import sys
from pydantic import BaseModel
from typing import Optional
import logging
from app.models.schemas import GribFile, GribsData, AtmosMetadata, WaveMetadata

# Base URL for GFS prod directory
prod_url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"

# Headers to mimic a browser request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Local directories to save downloaded files
base_dir = "gribs"
atmos_download_dir = os.path.join(base_dir, "atmos")
wave_download_dir = os.path.join(base_dir, "wave")
for directory in (base_dir, atmos_download_dir, wave_download_dir):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Files to store state and metadata
state_file = "polling.json"
gribs_file = os.path.join(base_dir, "gribs.json")

# Polling interval (seconds) and timeout (48 hours to handle date rollover)
poll_interval = 300  # 5 minutes
timeout_hours = 48
timeout = datetime.now() + timedelta(hours=timeout_hours)

# Global stop event for graceful shutdown
stop_event = threading.Event()

# Global event for GRIB file updates
gribs_updated_event = threading.Event()

def quit(signo, _frame):
    """Handle shutdown signals"""
    print(f"\nInterrupted by signal {signo}, shutting down...")
    stop_event.set()

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
                set(state.get('downloaded_atmos_files', [])),
                set(state.get('downloaded_wave_files', [])),
                state.get('is_downloading', False),
                state.get('last_update')
            )
    except FileNotFoundError:
        return None, None, set(), set(), False, None

def save_state(latest_date, latest_cycle, downloaded_atmos_files, downloaded_wave_files, is_downloading=False):
    """Save polling state to file"""
    state = {
        'latest_date': latest_date,
        'latest_cycle': latest_cycle,
        'downloaded_atmos_files': sorted(list(downloaded_atmos_files)),
        'downloaded_wave_files': sorted(list(downloaded_wave_files)),
        'is_downloading': is_downloading,
        'last_update': datetime.now().isoformat()
    }
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)

def update_gribs_json(atmos_file=None, wave_file=None, atmos_download_time=None, wave_download_time=None):
    """Update gribs.json with the latest downloaded file info and metadata"""
    try:
        with open(gribs_file, 'r') as f:
            gribs_data = json.load(f)
    except FileNotFoundError:
        gribs_data = {'atmos': None, 'wave': None}

    if atmos_file:
        cycle = atmos_file.split('.')[1]  # e.g., t06z
        gribs_data['atmos'] = {
            'path': atmos_file,
            'download_time': atmos_download_time,
            'metadata': {
                'cycle': cycle,
                'resolution': '0p25',
                'forecast_hour': 'f000'
            }
        }

    if wave_file:
        cycle = wave_file.split('.')[1]  # e.g., t06z
        gribs_data['wave'] = {
            'path': wave_file,
            'download_time': wave_download_time,
            'metadata': {
                'cycle': cycle,
                'resolution': '0p16',
                'domain': 'global',
                'forecast_hour': 'f000'
            }
        }

    with open(gribs_file, 'w') as f:
        json.dump(gribs_data, f, indent=2)
    
    # Signal that GRIB files have been updated
    gribs_updated_event.set()
    gribs_updated_event.clear()  # Reset the event for next update

def load_gribs_metadata() -> GribsData:
    """Load metadata from gribs.json and return it as a Pydantic GribsData object"""
    try:
        with open(gribs_file, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        # Return empty GribsData if file doesn't exist
        return GribsData()

    # Convert raw JSON to Pydantic model
    atmos_data = data.get('atmos')
    wave_data = data.get('wave')

    atmos = None
    if atmos_data:
        atmos = GribFile(
            path=atmos_data['path'],
            download_time=atmos_data['download_time'],
            metadata=AtmosMetadata(**atmos_data['metadata'])
        )

    wave = None
    if wave_data:
        wave = GribFile(
            path=wave_data['path'],
            download_time=wave_data['download_time'],
            metadata=WaveMetadata(**wave_data['metadata'])
        )

    return GribsData(atmos=atmos, wave=wave)

def download_file(file_url, local_path):
    """Download a file from a URL to a local path and return download time"""
    print(f"Downloading {file_url} to {local_path}")
    file_response = requests.get(file_url, headers=headers, stream=True)
    file_response.raise_for_status()
    with open(local_path, 'wb') as f:
        for chunk in file_response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    download_time = datetime.now().isoformat()
    print(f"Downloaded {os.path.basename(local_path)} at {download_time}")
    return download_time

def poll_gfs_data():
    """
    Poll for GFS f000 files (atmos and wave) and download them when available.
    Updates gribs.json with file paths, download times, and metadata.
    """
    latest_date, latest_cycle, downloaded_atmos_files, downloaded_wave_files, is_downloading, last_update = load_state()
    if not latest_date:
        print(f"No previous state found in {state_file}. Starting fresh.")
    else:
        print(f"Resumed state from {state_file}:")
        print(f"Latest date: {latest_date}, Latest cycle: {latest_cycle}")
        print(f"Atmos files downloaded: {len(downloaded_atmos_files)}")
        print(f"Wave files downloaded: {len(downloaded_wave_files)}")

    if latest_date and latest_cycle:
        cycle_time = f"t{latest_cycle}z"
        atmos_base_url = f"{prod_url}{latest_date}/{latest_cycle}/atmos/"
        wave_base_url = f"{prod_url}{latest_date}/{latest_cycle}/wave/gridded/"
        atmos_target_file = f"gfs.{cycle_time}.pgrb2.0p25.f000"
        wave_target_file = f"gfswave.{cycle_time}.global.0p16.f000.grib2"
        print(f"Resumed with atmos_base_url: {atmos_base_url}")
        print(f"Resumed with wave_base_url: {wave_base_url}")
    else:
        atmos_base_url = None
        wave_base_url = None
        cycle_time = None
        atmos_target_file = None
        wave_target_file = None

    print(f"Starting polling for GFS f000 files at {prod_url}")
    print(f"Polling every {poll_interval} seconds, timeout after {timeout_hours} hours")
    print("Targeting: Atmos (gfs.tXXz.pgrb2.0p25.f000) and Wave (gfswave.tXXz.global.0p16.f000.grib2)")

    while not stop_event.is_set():
        try:
            save_state(latest_date, latest_cycle, downloaded_atmos_files, downloaded_wave_files, is_downloading=True)

            response = requests.get(prod_url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            date_dirs = [link.text.strip('/') for link in soup.find_all('a') if link.text.startswith('gfs.')]
            if not date_dirs:
                print("No date directories found. Retrying after delay...")
                if stop_event.wait(poll_interval):
                    break
                continue

            new_date = max(date_dirs, key=lambda d: datetime.strptime(d, 'gfs.%Y%m%d'))
            if new_date != latest_date:
                latest_date = new_date
                print(f"\nNew date directory detected: {latest_date}")
                latest_cycle = None
                save_state(latest_date, latest_cycle, downloaded_atmos_files, downloaded_wave_files, is_downloading=True)

            date_url = f"{prod_url}{latest_date}/"
            response = requests.get(date_url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            cycles = [link.text.strip('/') for link in soup.find_all('a') if link.text.strip('/').isdigit()]
            if not cycles:
                print(f"No forecast cycles found in {latest_date}. Retrying after delay...")
                if stop_event.wait(poll_interval):
                    break
                continue

            new_cycle = max(cycles, key=int)
            if new_cycle != latest_cycle:
                latest_cycle = new_cycle
                cycle_time = f"t{latest_cycle}z"
                atmos_base_url = f"{date_url}{latest_cycle}/atmos/"
                wave_base_url = f"{date_url}{latest_cycle}/wave/gridded/"
                atmos_target_file = f"gfs.{cycle_time}.pgrb2.0p25.f000"
                wave_target_file = f"gfswave.{cycle_time}.global.0p16.f000.grib2"
                downloaded_atmos_files = {f for f in downloaded_atmos_files if not f.startswith(f"gfs.{cycle_time}")}
                downloaded_wave_files = {f for f in downloaded_wave_files if not f.startswith(f"gfswave.{cycle_time}")}
                print(f"\nNew forecast cycle detected: {latest_date}/{latest_cycle}Z")
                print(f"Atmos base URL: {atmos_base_url}")
                print(f"Wave base URL: {wave_base_url}")
                print(f"Targeting: {atmos_target_file} and {wave_target_file}")
                save_state(latest_date, latest_cycle, downloaded_atmos_files, downloaded_wave_files, is_downloading=True)

            for data_type, base_url, download_dir, downloaded_files, target_file in [
                ("Atmos", atmos_base_url, atmos_download_dir, downloaded_atmos_files, atmos_target_file),
                ("Wave", wave_base_url, wave_download_dir, downloaded_wave_files, wave_target_file)
            ]:
                try:
                    print(f"Attempting to access {data_type} URL: {base_url}")
                    response = requests.get(base_url, headers=headers)
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    if response.status_code == 403:
                        print(f"403 Forbidden error accessing {base_url}. This cycle may not be fully available yet.")
                        continue
                    raise

                soup = BeautifulSoup(response.text, 'html.parser')
                available_files = {link.text for link in soup.find_all('a') if target_file in link.text}
                if target_file in available_files and target_file not in downloaded_files:
                    print(f"{data_type}: Found target file: {target_file}")
                    file_url = base_url + target_file
                    local_path = os.path.join(download_dir, target_file)
                    download_time = download_file(file_url, local_path)
                    downloaded_files.add(target_file)
                    if data_type == "Atmos":
                        update_gribs_json(atmos_file=local_path, atmos_download_time=download_time)
                    else:
                        update_gribs_json(wave_file=local_path, wave_download_time=download_time)
                    save_state(latest_date, latest_cycle, downloaded_atmos_files, downloaded_wave_files, is_downloading=True)

            atmos_done = atmos_target_file in downloaded_atmos_files
            wave_done = wave_target_file in downloaded_wave_files
            if atmos_done and wave_done:
                print(f"Both target files for {latest_date}/{latest_cycle}Z downloaded successfully!")
                save_state(latest_date, latest_cycle, downloaded_atmos_files, downloaded_wave_files, is_downloading=False)
                break

            if datetime.now() > timeout:
                print("Timeout reached. Stopping polling.")
                print(f"Atmos: {'Downloaded' if atmos_done else 'Not downloaded'} ({atmos_target_file})")
                print(f"Wave: {'Downloaded' if wave_done else 'Not downloaded'} ({wave_target_file})")
                save_state(latest_date, latest_cycle, downloaded_atmos_files, downloaded_wave_files, is_downloading=False)
                break

            print(f"Waiting {poll_interval} seconds before next poll... "
                  f"(Atmos: {'Yes' if atmos_done else 'No'}, Wave: {'Yes' if wave_done else 'No'})")
            if stop_event.wait(poll_interval):
                break

        except requests.RequestException as e:
            print(f"Error during polling: {e}")
            print("Retrying after delay...")
            if stop_event.wait(poll_interval):
                break
        except Exception as e:
            print(f"Unexpected error: {e}")
            save_state(latest_date, latest_cycle, downloaded_atmos_files, downloaded_wave_files, is_downloading=False)
            break

    print("\nPolling complete.")
    if atmos_target_file not in downloaded_atmos_files:
        print(f"Atmos file not downloaded: {atmos_target_file}")
    if wave_target_file not in downloaded_wave_files:
        print(f"Wave file not downloaded: {wave_target_file}")
    save_state(latest_date, latest_cycle, downloaded_atmos_files, downloaded_wave_files, is_downloading=False)

def start_polling():
    """Start the GFS polling in a separate thread"""
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)
    signal.signal(signal.SIGHUP, quit)
    polling_thread = threading.Thread(target=poll_gfs_data, daemon=True)
    polling_thread.start()
    return polling_thread

if __name__ == "__main__":
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)
    signal.signal(signal.SIGHUP, quit)
    poll_gfs_data()

    # Example usage of load_gribs_metadata
    metadata = load_gribs_metadata()
    print("\nLoaded metadata from gribs.json:")
    if metadata.atmos:
        print(f"Atmos: {metadata.atmos.path}, Downloaded: {metadata.atmos.download_time}")
        print(f"  Metadata: {metadata.atmos.metadata}")
    if metadata.wave:
        print(f"Wave: {metadata.wave.path}, Downloaded: {metadata.wave.download_time}")
        print(f"  Metadata: {metadata.wave.metadata}")