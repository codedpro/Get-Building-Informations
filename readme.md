# Get Building Information

This repository contains a Python script for fetching and processing building data from a remote API using latitude and longitude values stored in a CSV file. The script is designed to handle large datasets efficiently by leveraging asynchronous requests (`aiohttp`) and chunked processing with `pandas`.

## Features

- **Asynchronous API Requests**: Fetch building data in parallel using `aiohttp` for high performance.
- **Chunked Processing**: Read and process large CSV files in chunks to save memory.
- **Retry Mechanism**: Automatically retries failed requests up to a defined limit.
- **Duplicate Prevention**: Avoids duplicate entries by maintaining a global set of building IDs.
- **Customizable Configuration**: Easily adjustable chunk size, retry limits, and headers.
- **JSON Output**: Saves the retrieved building data to a JSON file.

## Requirements

- Python 3.8 or higher
- Libraries: `pandas`, `aiohttp`, `asyncio`, `tqdm`, `ssl`, `json`, `os`

Install the required Python libraries:

```bash
pip install pandas aiohttp tqdm
```

## Usage

1. **Prepare the CSV File**:

   - Ensure your input CSV file contains `Lat` (latitude) and `Long` (longitude) columns. Update the `CSV_FILENAME` variable with the file name.

2. **Set Your API Key**:

   - Replace `YOUR_API_KEY_HERE` in the `HEADERS` dictionary with your API key.

3. **Run the Script**:

   - Execute the script using Python:
     ```bash
     python main.py
     ```

4. **Output**:
   - Successfully retrieved building data will be stored in `buildings.json`.

## Configuration

- **Chunk Size**: Adjust the number of rows processed per batch by modifying `CHUNK_SIZE`.
- **Retry Limit**: Set the maximum number of retries for a failed request with `MAX_FAILURES`.

## Example Workflow

1. The script reads the CSV file in chunks of `CHUNK_SIZE` rows.
2. For each row, it sends an API request to fetch building data.
3. Successfully retrieved buildings are stored in `buildings.json`.
4. Failed rows are retried up to `MAX_FAILURES` times.
5. Statistics are displayed at the end, including total retrieved buildings and permanently failed rows.

## License

This project is open-source and free to use.
