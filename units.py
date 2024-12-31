import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

buildings_file = 'buildings_array.json'
building_ids = set()

with open(buildings_file, 'r', encoding='utf-8') as f:
    buildings_data = json.load(f)
    for building in buildings_data:
        building_id = building.get('id')
        if building_id is not None:
            building_ids.add(building_id)

units_output_file = 'units.json'

headers = {
    'Content-Type': 'application/json',
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0',
    'Referer': 'https://gnaf2.post.ir/',
    'x-api-key': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImUyY2FmNjM2YmE0ZDIwZTIyYTBlMGUwYTIxZTI0NjkxNDFiODUxZmZjYjUxZjcyMjc2YmJkMDk2ZjEzNmRkOWU1YzdhZTQyZDg4NzFhZjQyIn0.eyJhdWQiOiIxIiwianRpIjoiZTJjYWY2MzZiYTRkMjBlMjJhMGUwZTBhMjFlMjQ2OTE0MWI4NTFmZmNiNTFmNzIyNzZiYmQwOTZmMTM2ZGQ5ZTVjN2FlNDJkODg3MWFmNDIiLCJpYXQiOjE2MTgyMjE4NTAsIm5iZiI6MTYxODIyMTg1MCwiZXhwIjoxNjIwOTg2NjUwLCJzdWIiOiIiLCJzY29wZXMiOlsiYmFzaWMiXX0.t_-fhdBc7OCQ6SkUDzRopb_uTQao7JaYUBXPCfes0XO-ljBKTdVg9c4QoXl21l8i7v48CGuvLbcgK78g-t8PkLc5OblOnHBLoEkgMMG4SVra1LG7TaqG78KIDldiJdX4Mi-JK8wIW0fayvfPrCense6Lv8crzh-Na_21DJ2CUWUrKRYiGpe6MrnkyC61V5JdkmmTTEUWyW5BpCdUJMg5-YHg1WN9gMwJGD40srrw1dlm8ouPtJNnXLeICgcXujz69uEe6cogGE9_31FJz-ujW83CWZMmDAHcoiQ1uH4CbOgElTvI3a7n8NvUMcQohqNMvb-GErZkn8dwOzsZY-2-WQ',  # Replace with your actual x-api-key
    'token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxIiwic3ViIjoiMTcwODFmNmYtY2JiOS00ODJiLThiMWMtMjEzZDYzN2E3NTNlIiwic2NvcGUiOlsiYmFzaWMiXSwiaXNzIjoiaHR0cDovL2duYWYyLnBvc3QuaXIvbG9naW4iLCJpYXQiOjE3MzI1MTk4MTgsImV4cCI6MTczMjUyMzQxOCwibmJmIjoxNzMyNTE5ODE4LCJqdGkiOiJuOFVXNWJOaUZhYmFpQnhGIn0.Akz4WNBWPjGLDe8QDLSD_Wx-v1cnhJG0ElxPgyryREKKzWGXIYNn5vNYArNpihUpidMm7RcdoRLsJOLou2TwgE6DimHXIxDl4pOjDidZl_8ozvbIR_K8aH4QQuwF9hNiJvQACXxlAchi2H0-D-JErYKlSaWZsxylofKYTFIyAWdZoTrRsGMV3vyT5DI8tX1v0rytGsf19ikYLj95BLlc3gSIkL44aQhYowam4QMRS3sbMDzyrby_-leP8URhmQFpzVLYRiCMQEazBROQfrpNLu7ebNusWxXe5793q4yTEPtDMsfwHXct0O5TMR2pnoInwTPpqpLvmCS8GwNpDyEzng'           # Replace with your actual token
}
def fetch_units_data(building_id):
    url = f'https://gnaf2.post.ir/post-services/buildings/{building_id}/units'
    try:
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code == 200:
            data = response.json()
            units = data.get('value', [])
            return units
        else:
            print(f'Error fetching units for building {building_id}: status code {response.status_code}')
    except Exception as e:
        print(f'Error fetching units for building {building_id}: {e}')
    return []

def main():
    units_data = []
    unit_ids = set()
    max_workers = 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_building_id = {executor.submit(fetch_units_data, building_id): building_id for building_id in building_ids}

        pbar = tqdm(total=len(future_to_building_id), desc="Fetching Units")

        for future in as_completed(future_to_building_id):
            building_id = future_to_building_id[future]
            units = future.result()
            for unit in units:
                unit_id = unit.get('id')
                if unit_id not in unit_ids:
                    units_data.append(unit)
                    unit_ids.add(unit_id)
            pbar.update(1)

        pbar.close()

    with open(units_output_file, 'w', encoding='utf-8') as f:
        json.dump(units_data, f, ensure_ascii=False, indent=4)

if __name__ == '__main__':
    main()
