import pandas as pd
# Replace with your file directory path
# Read CSV files
file_path = 'PDSI_mean.csv'
data = pd.read_csv(file_path)
output_file_path = ''
###################################################

# Create a new DataFrame to store the processed data
processed_data = data.copy()

# Starting from column B (index 1), every four columns of data are processed as a set
for start_col in range(1, data.shape[1], 4):
    group = data.iloc[:, start_col:start_col + 4]

    if (group == -999).all(axis=1).all():
        # If the whole set is -999, keep -999
        processed_data[group.columns] = -999
    else:
        # Overwrite other cells with values in the group that are not -999
        valid_value = group.apply(
            lambda row: row[row != -999].values[0] if any(row != -999) else -999, axis=1)
        for col in group.columns:
            processed_data[col] = valid_value

# Save processed data to a new CSV file
processed_data.to_csv(output_file_path, index=False)

output_file_path
