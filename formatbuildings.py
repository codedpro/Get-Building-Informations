import json

input_filename = 'buildings.json'
output_filename = 'buildings_array.json' 

buildings = []

with open(input_filename, 'r', encoding='utf-8') as infile:
    for line in infile:
        line = line.strip()
        if line:
            try:
                building = json.loads(line)
                buildings.append(building)
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON line: {e}")

with open(output_filename, 'w', encoding='utf-8') as outfile:
    json.dump(buildings, outfile, ensure_ascii=False, indent=4)
