import pandas as pd
import aiohttp
import asyncio
import json
import os
import ssl
from urllib.parse import urlencode
from tqdm.asyncio import tqdm_asyncio

CHUNK_SIZE = 100_000
MAX_FAILURES = 3 
CSV_FILENAME = 'Total-Isfahan-Points.csv'
JSON_FILENAME = 'buildings.json'
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0',
    'Referer': 'https://gnaf2.post.ir/',
    'x-api-key': 'YOUR_API_KEY_HERE'
}

ssl_context = ssl.create_default_context()
ssl_context.set_ciphers('DEFAULT')
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

building_ids = set()


def load_existing_buildings(json_filename: str):
    """
    Loads previously stored buildings from JSON filename line by line
    or from a JSON array. Adjust to your format as needed.
    """
    if not os.path.exists(json_filename):
        return [], set()

    with open(json_filename, 'r', encoding='utf-8') as f:
        try:
            existing_buildings = json.load(f)
            ids = {bld['id'] for bld in existing_buildings if 'id' in bld}
            return existing_buildings, ids
        except json.JSONDecodeError:
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
                    return False, "404 Not Found", []
                else:
                    return False, f"Non-200 response: Status {response.status}", []
        except Exception as e:
            pass 

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
    points = []
    for idx, row in df_chunk.iterrows():
        try:
            lat = row['Lat']
            lon = row['Long']
            points.append((idx, lat, lon))
        except KeyError:
            fail_counts[idx] = MAX_FAILURES 
            continue

    tasks = [
        fetch_building_data(session, idx, lat, lon, lock)
        for (idx, lat, lon) in points
    ]

    failed_rows = []
    new_buildings = []

    for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Processing Batch"):
        success, reason, buildings = await coro
        i = points[0][0] 
        points.pop(0)

        if success:
            new_buildings.extend(buildings) 
            if i in fail_counts:
                fail_counts.pop(i) 
        else:
            fail_counts[i] = fail_counts.get(i, 0) + 1
            failed_rows.append((i, reason))

    return failed_rows, new_buildings


async def main():

    existing_buildings, existing_ids = load_existing_buildings(JSON_FILENAME)
    building_ids.update(existing_ids)
    all_buildings = existing_buildings  
    fail_counts = {}

    connector = aiohttp.TCPConnector(limit=100, ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        lock = asyncio.Lock()

        chunk_iter = pd.read_csv(CSV_FILENAME, chunksize=CHUNK_SIZE)
        batch_number = 0

        for df_chunk in chunk_iter:
            batch_number += 1
            print(f"\n--- Processing batch #{batch_number}, size {len(df_chunk)} ---")

            for pass_num in range(2):
                print(f"Batch {batch_number} - Pass {pass_num+1}")
                failed_rows, new_buildings = await process_batch(df_chunk, session, lock, fail_counts)
                if new_buildings:
                    all_buildings.extend(new_buildings)

                df_chunk = df_chunk.loc[
                    [idx for idx in df_chunk.index if fail_counts.get(idx, 0) < MAX_FAILURES]
                ]

                if not failed_rows or df_chunk.empty:
                    break

            print(f"Saving buildings after batch #{batch_number} ...")
            save_buildings_to_json(all_buildings, JSON_FILENAME)

        permanently_failed = [idx for (idx, fails) in fail_counts.items() if fails >= MAX_FAILURES]

        print("\n--- Processing complete! ---")
        print(f"Total buildings in JSON: {len(all_buildings)}")
        print(f"Total permanently failed rows: {len(permanently_failed)}")

        if len(permanently_failed) > 0:
            answer = input("Do you want to retry the permanently failed rows from scratch? (y/n): ")
            if answer.lower().startswith('y'):
                df_failed = pd.read_csv(CSV_FILENAME, usecols=['Lat', 'Long'], index_col=None)
                df_failed = df_failed.loc[permanently_failed]
                
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
                
                save_buildings_to_json(all_buildings, JSON_FILENAME)
                permanently_failed = [idx for (idx, fails) in fail_counts.items() if fails >= MAX_FAILURES]
                print(f"After re-try, total permanently failed rows: {len(permanently_failed)}")
            if permanently_failed:
                df_all = pd.read_csv(CSV_FILENAME, index_col=None)
                df_final_failed = df_all.loc[permanently_failed].copy()
                df_final_failed['Failure Count'] = df_final_failed.index.map(fail_counts)
                df_final_failed.to_csv("final_failed_rows.csv", index=False)
                print("Final failed rows saved to final_failed_rows.csv")


if __name__ == '__main__':
    asyncio.run(main())
