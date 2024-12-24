import pandas as pd
import os

def split_csv(file_name: str, num_pieces: int, output_prefix: str):
    """
    Splits a CSV file into a specified number of pieces and saves them to separate files.

    Args:
        file_name (str): The name of the CSV file to split.
        num_pieces (int): The number of pieces to split the file into.
        output_prefix (str): The prefix for the output files.

    Returns:
        None
    """
    try:
        # Read the entire CSV file
        df = pd.read_csv(file_name)
        
        # Calculate the number of rows per piece
        num_rows = len(df)
        rows_per_piece = num_rows // num_pieces
        remainder = num_rows % num_pieces
        
        print(f"Total rows: {num_rows}, Rows per piece: {rows_per_piece}, Remainder: {remainder}")
        
        # Split and save each piece
        start_row = 0
        for i in range(num_pieces):
            # Calculate end row for this piece
            end_row = start_row + rows_per_piece + (1 if i < remainder else 0)
            piece_df = df.iloc[start_row:end_row]
            
            # Save the piece to a new file
            output_file = f"{output_prefix}_part_{i + 1}.csv"
            piece_df.to_csv(output_file, index=False)
            print(f"Saved piece {i + 1} to {output_file}")
            
            # Update the start row for the next piece
            start_row = end_row
    except Exception as e:
        print(f"Error: {e}")

# Example usage
file_name = "Teh-Alborz.csv"  # Name of the input CSV file
num_pieces = 10  # Number of pieces to split the file into
output_prefix = "Teh-Alborz_split"  # Prefix for the output files

split_csv(file_name, num_pieces, output_prefix)
