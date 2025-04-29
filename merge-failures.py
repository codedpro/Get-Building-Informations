import pandas as pd
import glob
import os

directory = ""

# Define the pattern to match all CSV files (allowing any pass number)
file_pattern = os.path.join(directory, "intermediate_failures_batch*_pass*.csv")

# Get a list of all matching CSV files
csv_files = glob.glob(file_pattern)

# Check if files are found
if not csv_files:
    print("No matching files found. Please check the file names and directory.")
else:
    # Read and concatenate all CSV files into a single DataFrame
    df_combined = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)

    # Save the combined data into a new CSV file
    output_file = os.path.join(directory, "failures.csv")
    df_combined.to_csv(output_file, index=False)

    print(f"All CSV files have been merged successfully into {output_file}!")
