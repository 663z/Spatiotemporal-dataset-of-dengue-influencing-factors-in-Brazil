import os
import pandas as pd


# Replace with your file directory path
file_directory = ''   
outputpath = ''
output_file_path = os.path.join(outputpath, '.csv')
################################################################

# Get all files in the directory
files = [f for f in os.listdir(file_directory) if f.endswith('.csv')]

# Initialize an empty DataFrame to store the results of the merge
merged_data = pd.DataFrame()

# Iterate through each document
for file in files:
    # Read CSV files
    file_path = os.path.join(file_directory, file)
    data = pd.read_csv(file_path, header=0)
    
    # Extract the 5 digits of the filename
    file_id = ''.join(filter(str.isdigit, file))
    
    # Rename columns
    data = data.rename(columns={'weekly_rainy_days': file_id})
    # View column names
    print(data.columns)  
    
    # Setting the date as an index
    data = data.set_index('date')
    
    # Merge data
    if merged_data.empty:
        merged_data = data
    else:
        merged_data = merged_data.join(data, how='outer')

# Transpose the DataFrame so that dates become column labels and file IDs become row labels
merged_data = merged_data.T

# Save the merged data to a new CSV file
merged_data.to_csv(output_file_path)

print(f'Merged data saved to {output_file_path}')