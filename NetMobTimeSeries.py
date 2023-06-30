import os
import pandas as pd

# NetMob的txt数据按应用和ULDL统计多日time stamps交通总量。
# Set the directory paths for input and output
txt_input_directory = '/Users/zhangkunyi/Downloads/Paris'
grouped_output_directory = '/Users/zhangkunyi/Downloads/ParisGrouped'

# Create the output directories if they don't exist
if not os.path.exists(grouped_output_directory):
    os.makedirs(grouped_output_directory)

# Use a dict to hold the aggregated DataFrames, keyed by service and direction
dfs = {}

# Use a dict to keep track of the order of appearance of each date
date_order = {}

# Create a list to store txt files
txt_files = []

# Gather txt files and their paths
for root, dirs, files in os.walk(txt_input_directory):
    for file_name in files:
        if file_name.endswith('.txt'):
            txt_files.append((root, file_name))

# Sort txt files by date
txt_files.sort(key=lambda item: item[1].split('_')[-2])

# Convert txt files to csv files
for root, file_name in txt_files:
    # Construct the input file path
    input_file_path = os.path.join(root, file_name)

    # Open the txt file for reading
    with open(input_file_path, 'r') as txt_file:
        # Read data into list of lists
        data = [line.strip().split(' ') for line in txt_file]

        # # Convert data to DataFrame
        # df = pd.DataFrame(data, columns=['AreaID'] + [f'Volume_{i}' for i in range(1, 97)])
        num_columns = len(data[0])  # Assuming all rows have the same number of columns
        df = pd.DataFrame(data, columns=['AreaID'] + [f'Volume_{i}' for i in range(1, num_columns)])

        # Make sure to convert Volume columns to integer
        for col in df.columns[1:]:
            df[col] = df[col].astype(int)

        # Compute sum and convert to DataFrame
        # sum_values = df.iloc[:, 1:97].sum()
        sum_values = df.iloc[:, 1:num_columns].sum()
        sum_df = sum_values.reset_index()
        sum_df['index'] = sum_df['index'].str.extract('(\d+)')
        sum_df.columns = ['Time Stamp', 'Sum Value']

        # Make sure to convert 'Time Stamp' and 'Sum Value' columns to integer
        sum_df['Time Stamp'] = sum_df['Time Stamp'].astype(int)
        sum_df['Sum Value'] = sum_df['Sum Value'].astype(int)

        # Split the file name into parts
        parts = file_name.split('_')

        # Get the date and direction (last two parts)
        date = parts[-2]
        direction = parts[-1]

        # The service name is everything before the date and direction
        service = '_'.join(parts[:-2])

        # Define the group key
        group_key = (service, direction)

        # If this date hasn't been seen before, add it to the date_order dict
        if date not in date_order:
            date_order[date] = len(date_order)

        # Adjust the "Time Stamp" values
        # sum_df['Time Stamp'] += date_order[date] * 96
        num_periods_per_day = num_columns - 1  # Minus 1 for the 'AreaID' column
        sum_df['Time Stamp'] += date_order[date] * num_periods_per_day
        
        # If this group hasn't been seen before, add the DataFrame to the dict
        if group_key not in dfs:
            dfs[group_key] = sum_df
        else:
            # Otherwise, concatenate the new data with the existing DataFrame
            dfs[group_key] = pd.concat([dfs[group_key], sum_df])

# Sort by "Time Stamp", and save each aggregated DataFrame to a new CSV file
for (service, direction), df in dfs.items():
    df.sort_values('Time Stamp', inplace=True)
    output_file_path = os.path.join(grouped_output_directory, f'{service}_{direction}.csv')
    df.to_csv(output_file_path, index=False)

print("Processing completed.")