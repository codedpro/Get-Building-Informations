import pandas as pd
import aiohttp
import asyncio
import json
import os
import ssl
from urllib.parse import urlencode
from tqdm.asyncio import tqdm_asyncio

# ----------------------------
# Configuration
# ----------------------------
CHUNK_SIZE = 100_000
MAX_FAILURES = 3  # Number of times a single row can fail before we abandon it
CSV_FILENAME = 'Total-Isfahan-Points.csv'
JSON_FILENAME = 'buildings.json'
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0',
    'Referer': 'https://gnaf2.post.ir/',
    'x-api-key': 'YOUR_API_KEY_HERE'
}

# Create an SSL context to skip verification
ssl_context = ssl.create_default_context()
ssl_context.set_ciphers('DEFAULT')
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# ----------------------------
# Global Sets & Helpers
# ----------------------------
building_ids = set()  # to avoid duplicates in JSON


def load_existing_buildings(json_filename: str):
    """
    Loads previously stored buildings from JSON filename line by line
    or from a JSON array. Adjust to your format as needed.
    """
    if not os.path.exists(json_filename):
        return [], set()

    with open(json_filename, 'r', encoding='utf-8') as f:
        try:
            # If your JSON is stored as a list:
            existing_buildings = json.load(f)
            ids = {bld['id'] for bld in existing_buildings if 'id' in bld}
            return existing_buildings, ids
        except json.JSONDecodeError:
            # fallback if your JSON was line by line or other format
            # you can adapt your reading logic as needed
            existing_buildings = []
            ids = set()
            f.seek(0)
            for line in f:
                try:
                    building = json.loads(line)
                    if isinstance(building, dict) and 'id' in building:
                        existing_buildings.append(building)
                        ids.add(building['id'])
                except json.JSONDecodeError:
                    continue
            return existing_buildings, ids


def save_buildings_to_json(buildings, json_filename: str):
    """
    Saves the buildings to JSON as a single array (list of dicts).
    Overwrites the file each time. 
    """
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(buildings, f, ensure_ascii=False, indent=4)


def extract_buildings(data):
    """
    Extract the building info out of the JSON response.
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
    Fetch building data for a single row (lat, lon).
    Return (success: bool, reason: str or None, new_buildings: list).
    """
    params = {
        '$top': '20',
        '$filter': f'contains(parcel, POINT({lon} {lat}))'
    }
    url = 'https://gnaf2.post.ir/post-services/buildings?' + urlencode(params)

    # We'll do a local retry (like you had attempt in the older code),
    # but note we also do a higher-level "batch retry" approach.
    # You can decide to remove or keep local attempt logic.
    for attempt in range(3):
        try:
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                        buildings = extract_buildings(data)
                        if buildings is None:
                            return False, "Extracted buildings is None", []
                        new_buildings = []
                        async with lock:
                            for bld in buildings:
                                b_id = bld.get('id')
                                if b_id and (b_id not in building_ids):
                                    new_buildings.append(bld)
                                    building_ids.add(b_id)
                        return True, None, new_buildings
                    except json.JSONDecodeError as json_error:
                        return False, f"JSON decode error: {json_error}", []
                elif response.status == 404:
                    # If you want to handle 404 differently, you can
                    return False, "404 Not Found", []
                else:
                    return False, f"Non-200 response: Status {response.status}", []
        except Exception as e:
            # network or other exception
            pass  # We'll let it try again

    # If all attempts fail
    return False, "All attempts failed after local retries", []


async def process_batch(df_chunk, session, lock, fail_counts):
    """
    Process one batch (DataFrame chunk).
      - df_chunk: The subset of rows (up to 100k).
      - session: AIOHTTP session
      - lock: An asyncio Lock
      - fail_counts: dictionary to keep track of how many times a row has failed
    Returns:
      - failed_rows: list of (index, reason)
      - new_buildings: list of newly retrieved buildings
    """
    tasks = []
    # We'll store each row's (index, lat, lon) so we can map them later
    points = []
    for idx, row in df_chunk.iterrows():
        try:
            lat = row['Lat']
            lon = row['Long']
            points.append((idx, lat, lon))
        except KeyError:
            # Means row doesn't have lat/long at all
            # We'll treat it as an immediate fail (no point in retrying)
            fail_counts[idx] = MAX_FAILURES  # mark it failed enough times
            continue

    # Build the coroutines
    tasks = [
        fetch_building_data(session, idx, lat, lon, lock)
        for (idx, lat, lon) in points
    ]

    failed_rows = []
    new_buildings = []

    for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Processing Batch"):
        success, reason, buildings = await coro
        # We need to see which index it corresponds to
        # We can figure that out by enumerating or storing references carefully.
        # The easiest is to just pop from a queue, or zip them. We'll do a quick approach:
        i = points[0][0]  # We assume tasks are done in the same order they are listed
        points.pop(0)

        if success:
            new_buildings.extend(buildings)  # accumulate new buildings
            if i in fail_counts:  # if it existed (it might not if 0 before)
                fail_counts.pop(i)  # success -> remove from fail_counts
        else:
            # increment fail count
            fail_counts[i] = fail_counts.get(i, 0) + 1
            failed_rows.append((i, reason))

    return failed_rows, new_buildings


async def main():
    # ------------------------
    # 1) Load existing buildings from JSON
    # ------------------------
    existing_buildings, existing_ids = load_existing_buildings(JSON_FILENAME)
    building_ids.update(existing_ids)
    all_buildings = existing_buildings  # We will keep appending to this

    # Keep a global dict: row_index -> times failed
    fail_counts = {}

    # ------------------------
    # 2) Create AIOHTTP session
    # ------------------------
    connector = aiohttp.TCPConnector(limit=100, ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        lock = asyncio.Lock()

        # ------------------------
        # 3) Read the CSV in chunks of 100k
        # ------------------------
        chunk_iter = pd.read_csv(CSV_FILENAME, chunksize=CHUNK_SIZE)
        batch_number = 0

        for df_chunk in chunk_iter:
            batch_number += 1
            print(f"\n--- Processing batch #{batch_number}, size {len(df_chunk)} ---")

            # Now we do a "batch processing loop" that can handle re-tries
            # but let's limit it to 1 or 2 passes. Adjust as you see fit.
            # The concept: we process the chunk -> some fails -> re-process
            # only the fails. If still fail -> move on or keep trying.
            # This is separate from the local 3 attempts in fetch_building_data.

            # We'll do up to 2 passes here:
            for pass_num in range(2):
                print(f"Batch {batch_number} - Pass {pass_num+1}")
                failed_rows, new_buildings = await process_batch(df_chunk, session, lock, fail_counts)
                # Accumulate new buildings
                if new_buildings:
                    all_buildings.extend(new_buildings)

                # Filter out from df_chunk those that have 3 or more fails
                # so they won't be retried in the next pass
                df_chunk = df_chunk.loc[
                    [idx for idx in df_chunk.index if fail_counts.get(idx, 0) < MAX_FAILURES]
                ]

                # If nothing failed or nothing left in chunk, break early
                if not failed_rows or df_chunk.empty:
                    break

            # At the end of this batch, we write out the (updated) buildings JSON
            print(f"Saving buildings after batch #{batch_number} ...")
            save_buildings_to_json(all_buildings, JSON_FILENAME)

            # If there are rows that have 3 or more fails, we won't attempt them again
            # We'll continue to next chunk. The 'fail_counts' remains in the dictionary
            # so we can track overall how many are permanently failed after each chunk.

        # Done with all chunks. Now we see how many total failed permanently
        permanently_failed = [idx for (idx, fails) in fail_counts.items() if fails >= MAX_FAILURES]

        # We can display stats:
        print("\n--- Processing complete! ---")
        print(f"Total buildings in JSON: {len(all_buildings)}")
        print(f"Total permanently failed rows: {len(permanently_failed)}")

        if len(permanently_failed) > 0:
            # If you want to re-try them from scratch, you can do so here.
            # For demonstration, let's ask the user:
            answer = input("Do you want to retry the permanently failed rows from scratch? (y/n): ")
            if answer.lower().startswith('y'):
                # Build a new DataFrame of just the fails
                df_failed = pd.read_csv(CSV_FILENAME, usecols=['Lat', 'Long'], index_col=None)
                df_failed = df_failed.loc[permanently_failed]
                
                # Reset their fail counts
                for idx in permanently_failed:
                    fail_counts[idx] = 0

                print("Retrying failed rows (one more pass)...")
                for pass_num in range(2):
                    failed_rows, new_buildings = await process_batch(df_failed, session, lock, fail_counts)
                    if new_buildings:
                        all_buildings.extend(new_buildings)
                    df_failed = df_failed.loc[
                        [idx for idx in df_failed.index if fail_counts.get(idx, 0) < MAX_FAILURES]
                    ]
                    if not failed_rows or df_failed.empty:
                        break
                
                # Final save
                save_buildings_to_json(all_buildings, JSON_FILENAME)

                # Re-check how many remain failed
                permanently_failed = [idx for (idx, fails) in fail_counts.items() if fails >= MAX_FAILURES]
                print(f"After re-try, total permanently failed rows: {len(permanently_failed)}")

            # If we still have fails left, we can save them to a CSV
            if permanently_failed:
                df_all = pd.read_csv(CSV_FILENAME, index_col=None)
                df_final_failed = df_all.loc[permanently_failed].copy()
                df_final_failed['Failure Count'] = df_final_failed.index.map(fail_counts)
                df_final_failed.to_csv("final_failed_rows.csv", index=False)
                print("Final failed rows saved to final_failed_rows.csv")


if __name__ == '__main__':
    asyncio.run(main())
