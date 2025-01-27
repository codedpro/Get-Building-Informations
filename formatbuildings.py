import json
import gzip

input_filename = 'buildings_West Azerbaijan.txt'
output_filename = 'building_West Azerbaijan.json.gz'

with open(input_filename, 'r', encoding='utf-8') as infile, gzip.open(output_filename, 'wt', encoding='utf-8') as outfile:
    outfile.write("[\n") 
    first_record = True

    for line in infile:
        line = line.strip()
        if line:
            try:
                building = json.loads(line)
                if not first_record:
                    outfile.write(",\n")
                else:
                    first_record = False
                json.dump(building, outfile, ensure_ascii=False)
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON line: {e}")

    outfile.write("\n]") 