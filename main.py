import pandas as pd
import aiohttp
import asyncio
import json
import os
import ssl
from urllib.parse import urlencode
from tqdm.asyncio import tqdm_asyncio
import argparse

CHUNK_SIZE = 50_000
MAX_FAILURES = 10
CSV_FILENAME = 'failures.csv'
NDJSON_FILENAME = 'buildings_miss.txt'
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0',
    'Referer': 'https://gnaf2.post.ir/',
    'x-api-key': 'YOUR_API_KEY_HERE'
}

PROGRESS_FILE = 'progress.txt'  # File to store the last processed batch number

ssl_context = ssl.create_default_context()
ssl_context.set_ciphers('DEFAULT')
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

building_ids = set()

def load_existing_buildings_ndjson(filename: str):
    """
    Load building IDs (and optionally building data) from an ND-JSON file.
    Each line is one JSON building object.
    Return a set of building IDs so we avoid duplicates.
    """
    if not os.path.exists(filename):
        return set()

    ids = set()
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                building = json.loads(line)
                b_id = building.get('id')
                if b_id:
                    ids.add(b_id)
            except json.JSONDecodeError:
                continue
    return ids

def append_buildings_to_ndjson(buildings, filename: str):
    """
    Append new buildings to an ND-JSON file (one building per line).
    """
    if not buildings:
        return

    with open(filename, 'a', encoding='utf-8') as f:
        for bld in buildings:
            f.write(json.dumps(bld, ensure_ascii=False) + "\n")

def extract_buildings(data):
    """
    Extract the building info out of the JSON response.
    Returns a list of dicts (each a building).
    """
    buildings = []
    value = data.get('value', [])
    if not value:
        return buildings
    for item in value:
        if isinstance(item, list):
            buildings.extend(item)
        elif isinstance(item, dict):
            buildings.append(item)
    return buildings

async def fetch_building_data(session, index, lat, lon, lock):
    """
    Fetch building data for a single (lat, lon).
    Return a tuple:
       (idx, success: bool, reason: str, new_buildings: list)
    """
    params = {
        '$top': '20',
        '$filter': f'contains(parcel, POINT({lon} {lat}))'
    }
    url = 'https://gnaf2.post.ir/post-services/buildings?' + urlencode(params)

    for attempt in range(3):
        try:
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                        buildings = extract_buildings(data)
                        if not buildings:
                            # We consider it a successful call but no buildings found
                            return (index, True, "No buildings found", [])
                        new_buildings = []
                        async with lock:
                            for bld in buildings:
                                b_id = bld.get('id')
                                if b_id and (b_id not in building_ids):
                                    new_buildings.append(bld)
                                    building_ids.add(b_id)
                        return (index, True, None, new_buildings)
                    except json.JSONDecodeError as json_error:
                        return (index, False, f"JSON decode error: {json_error}", [])
                elif response.status == 404:
                    return (index, False, "404 Not Found", [])
                else:
                    return (index, False, f"Non-200 response: Status {response.status}", [])
        except Exception as e:
            # You can optionally log the exception e here
            pass

    # If we exhausted all attempts
    return (index, False, "All attempts failed after local retries", [])

from tqdm.asyncio import tqdm

async def process_batch(df_chunk, session, lock, fail_counts, batch_number, pass_number):
    """
    Process one pass of the given df_chunk. Creates and runs tasks for each row in the chunk.

    Returns:
      - next_df_chunk: A filtered df_chunk that excludes rows that succeeded or are permanently failed
      - total_attempts: how many rows we attempted to process in this pass
      - successes: how many rows succeeded
      - zero_build_count: how many returned 0 buildings
      - new_bld_count: how many new buildings found
      - failures: how many rows failed in this pass (not cumulative)
    """
    points = []
    for idx, row in df_chunk.iterrows():
        try:
            lat = float(row['Lat'])
            lon = float(row['Long'])
            points.append((idx, lat, lon))
        except KeyError:
            # If lat/long columns are missing, treat as permanent fail
            fail_counts[idx] = MAX_FAILURES
            continue

    tasks = []
    for (idx, lat, lon) in points:
        task = fetch_building_data(session, idx, lat, lon, lock)
        tasks.append(task)

    total_attempts = len(tasks)
    successes = 0
    failures = 0
    zero_build_count = 0
    new_bld_count = 0

    # We'll keep track of indices that succeeded (so we remove them from next pass)
    succeeded_indices = []
    all_new_buildings = []

    # Use tqdm to track progress
    desc_str = f"Batch#{batch_number} Pass#{pass_number} - Processing"
    for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=desc_str):
        idx, success, reason, buildings = await coro

        if success:
            successes += 1
            if reason == "No buildings found":
                zero_build_count += 1
            else:
                # We have new buildings or reason=None
                if buildings:
                    new_bld_count += len(buildings)
                    all_new_buildings.extend(buildings)  # Collect new buildings for batch write
            # Mark row as succeeded (no need to re-check)
            succeeded_indices.append(idx)
            if idx in fail_counts:
                fail_counts.pop(idx, None)
        else:
            # Failed
            failures += 1
            fail_counts[idx] = fail_counts.get(idx, 0) + 1

    # Add new buildings to the ND-JSON file
    append_buildings_to_ndjson(all_new_buildings, NDJSON_FILENAME)

    # Handle failed rows properly
    valid_failed_indices = [idx for idx in fail_counts.keys() if idx in df_chunk.index]

    if valid_failed_indices:
        failed_rows = df_chunk.loc[valid_failed_indices].copy()
        failed_rows['Failure Count'] = failed_rows.index.map(fail_counts)
        failed_rows.to_csv(f"intermediate_failures_batch{batch_number}_pass{pass_number}.csv", index=False)
        print(f"Intermediate failures logged to intermediate_failures_batch{batch_number}_pass{pass_number}.csv")

    # Remove permanently failed rows from next pass
    permanently_failed_indices = [
        idx for idx, fcount in fail_counts.items() if fcount >= MAX_FAILURES
    ]

    # Remove successes and permanently failed from the next pass
    rows_to_drop = set(succeeded_indices + permanently_failed_indices)
    next_df_chunk = df_chunk.drop(rows_to_drop, errors='ignore')

    return next_df_chunk, total_attempts, successes, zero_build_count, new_bld_count, failures


async def main(start_batch):
    # 1) Load building IDs from ND-JSON to avoid duplicates
    global building_ids
    building_ids = load_existing_buildings_ndjson(NDJSON_FILENAME)

    # We'll track how many total zero-building calls we get for info
    total_zero_buildings = 0

    # Keep track of how many rows fail after multiple tries (key=index, value=count)
    fail_counts = {}

    # Set up aiohttp session
    connector = aiohttp.TCPConnector(limit=100, ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        lock = asyncio.Lock()

        # 2) Read the CSV in chunks
        chunk_iter = pd.read_csv(CSV_FILENAME, chunksize=CHUNK_SIZE)
        batch_number = 0

        for df_chunk in chunk_iter:
            batch_number += 1

            if batch_number < start_batch:
                print(f"Skipping batch #{batch_number} as it's before START_BATCH ({start_batch}).")
                continue

            print(f"\n=== Processing batch #{batch_number}, size {len(df_chunk)} ===")

            # We can run multiple passes on the same chunk to retry partial failures.
            # Let’s do 5 passes by default.
            for pass_num in range(1, 2):
                if df_chunk.empty:
                    print(f"Batch#{batch_number} Pass#{pass_num}: No rows left to process. Stopping passes.")
                    break

                (
                    next_df_chunk,
                    total_attempts,
                    successes,
                    zero_build_count,
                    new_bld_count,
                    failures
                ) = await process_batch(
                    df_chunk, session, lock, fail_counts,
                    batch_number, pass_num
                )

                # Logging for this pass
                print(f"Batch#{batch_number} Pass#{pass_num} Stats:")
                print(f"   Rows Attempted   = {total_attempts}")
                print(f"   Successes        = {successes} (including {zero_build_count} with zero buildings)")
                print(f"   Failures         = {failures}")
                print(f"   New Buildings    = {new_bld_count}")
                print(f"   Remaining in df  = {len(next_df_chunk)}")

                total_zero_buildings += zero_build_count

                with open(PROGRESS_FILE, 'w') as pf:
                    pf.write(str(batch_number))

                df_chunk = next_df_chunk

                if failures == 0 or df_chunk.empty:
                    print(f"Batch#{batch_number} Pass#{pass_num}: No more failures or empty chunk. Moving on.")
                    break

        # After processing all chunks, figure out which rows never succeeded
        permanently_failed = [
            idx for (idx, fails) in fail_counts.items() if fails >= MAX_FAILURES
        ]

        print("\n=== Processing complete! ===")
        print(f"Total unique building IDs in memory: {len(building_ids)}")
        print(f"Total calls that returned 0 buildings: {total_zero_buildings}")
        print(f"Total permanently failed rows: {len(permanently_failed)}")

        # Optional: Let the user retry permanently_failed rows from scratch
        if permanently_failed:
            answer = input("Do you want to retry the permanently failed rows from scratch? (y/n): ")
            if answer.lower().startswith('y'):
                # Load only Lat/Long for the failed rows
                df_failed = pd.read_csv(CSV_FILENAME, usecols=['Lat', 'Long'], index_col=None)
                # We only keep the rows that were marked as permanent fails
                # We need to align them by index if the CSV had a default range index
                # If your CSV has no index, permanently_failed might not match directly.
                # We'll assume the CSV index matches row numbering. If not, adapt accordingly.
                df_failed = df_failed.loc[permanently_failed]

                # Reset their fail counts
                for idx in permanently_failed:
                    fail_counts[idx] = 0

                print("Retrying failed rows (two more passes)...")
                for pass_num in range(1, 3):
                    if df_failed.empty:
                        print(f"Retry Pass#{pass_num}: No rows left to process.")
                        break

                    (
                        next_df_chunk,
                        total_attempts,
                        successes,
                        zero_build_count,
                        new_bld_count,
                        failures
                    ) = await process_batch(
                        df_failed, session, lock, fail_counts,
                        batch_number="Retry", pass_number=pass_num
                    )

                    print(f"Retry Pass#{pass_num} Stats:")
                    print(f"   Rows Attempted   = {total_attempts}")
                    print(f"   Successes        = {successes} (including {zero_build_count} zero-buildings)")
                    print(f"   Failures         = {failures}")
                    print(f"   New Buildings    = {new_bld_count}")

                    df_failed = next_df_chunk  # update for next pass
                    
                    if failures == 0 or df_failed.empty:
                        break

                # Re-check
                permanently_failed = [
                    idx for (idx, fails) in fail_counts.items() if fails >= MAX_FAILURES
                ]
                print(f"After re-try, total permanently failed rows: {len(permanently_failed)}")

            # Save them to a CSV for manual inspection if any remain
            if permanently_failed:
                df_all = pd.read_csv(CSV_FILENAME, index_col=None)
                # If the CSV's rows match 1:1 with default index, this works.
                # Otherwise adapt to your CSV's indexing scheme.
                df_final_failed = df_all.loc[permanently_failed].copy()
                df_final_failed['Failure Count'] = df_final_failed.index.map(fail_counts)
                df_final_failed.to_csv("final_failed_rows.csv", index=False)
                print("Final failed rows saved to final_failed_rows.csv")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process building data with batch resume capability.")
    parser.add_argument('--start-batch', type=int, default=1, help='Batch number to start processing from.')
    args = parser.parse_args()

    # Check if there's a progress file and set start_batch accordingly if not provided
    if args.start_batch == 1 and os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as pf:
            last_batch = pf.read().strip()
            if last_batch.isdigit():
                args.start_batch = int(last_batch) + 1
                print(f"Resuming from batch #{args.start_batch} based on progress file.")

    asyncio.run(main(start_batch=args.start_batch))
