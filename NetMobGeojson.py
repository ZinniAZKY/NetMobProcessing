import os
import pandas as pd
import geopandas as gpd
from datetime import datetime

# # Input directory
# input_directory = '/Users/zhangkunyi/Downloads/Paris'
# shapefile_path = "/Users/zhangkunyi/Downloads/Netmob/Paris/Paris.shp"
# output_file_path = "/Users/zhangkunyi/Downloads/ParisSum.geojson"
# # Master dataframe to store all data
# master_df = None
#
# # Loop through each txt file in the directory
# for root, dirs, files in os.walk(input_directory):
#     for file_name in files:
#         if file_name.endswith('.txt'):
#             # Construct the input file path
#             input_file_path = os.path.join(root, file_name)
#
#             # Load data from txt file into a DataFrame
#             df = pd.read_csv(input_file_path, sep=' ', header=None)
#
#             # First column is AreaID, others are volumes
#             df.columns = ['AreaID'] + [f'Volume_{i}' for i in range(1, 97)]
#
#             # Sum the volumes and add to a new column
#             df[f'{file_name.replace(".txt", "")}'] = df.iloc[:, 1:97].sum(axis=1)
#
#             # Drop the individual volume columns
#             df = df[['AreaID', f'{file_name.replace(".txt", "")}']]
#
#             # Merge to master dataframe
#             if master_df is None:
#                 master_df = df
#             else:
#                 master_df = pd.merge(master_df, df, on='AreaID', how='outer')
#
# # Convert the 'AreaID' column to integer
# master_df['AreaID'] = master_df['AreaID'].astype(int)
#
# # Load the shapefile
# shapefile = gpd.read_file(shapefile_path)
#
# # Ensure the data is joined on the correct ID
# shapefile.set_index('tile_id', inplace=True)
# master_df.set_index('AreaID', inplace=True)
#
# # Create a copy of the shapefile GeoDataFrame
# new_gdf = shapefile.copy()
#
# # Join the master_df dataframe to the GeoDataFrame
# new_gdf = new_gdf.join(master_df)
#
# # Export the GeoDataFrame to a new GeoJSON file
# new_gdf.to_file(output_file_path, driver='GeoJSON')
#
# print(f"GeoJSON file generated at: {output_file_path}")


# Input directory
input_directory = '/home/ubuntu/workspace/ABMDLZhang/NetMob/ParisDL/'
csv_output_file_path = "/home/ubuntu/workspace/ABMDLZhang/NetMob/ParisSumGDF.csv"

# A list to store all the DataFrames to be concatenated
df_list = []

# Date range
start_date = datetime.strptime('20180318', '%Y%m%d')
end_date = datetime.strptime('20190324', '%Y%m%d')

for root, dirs, files in os.walk(input_directory):
    folder_date_str = os.path.basename(root)

    # Attempt to parse folder name as a date
    try:
        folder_date = datetime.strptime(folder_date_str, '%Y%m%d')
    except ValueError:
        continue  # Skip if folder name is not a date

    # Check if folder date is within the desired range
    if start_date <= folder_date <= end_date:
        for file_name in files:
            if file_name.endswith('.txt'):
                # Construct the input file path
                input_file_path = os.path.join(root, file_name)

                # Extract service name from the directory structure
                service_name = os.path.basename(os.path.dirname(root))

                # Load data from txt file into a DataFrame
                df = pd.read_csv(input_file_path, sep=' ', header=None)

                # Dynamically assign column names based on number of columns in the file
                df.columns = ['AreaID'] + [f'Volume_{i}' for i in range(1, df.shape[1])]

                # Sum the volumes and add to a new column
                df[f'{service_name}'] = df.iloc[:, 1:].sum(axis=1)

                # Drop the individual volume columns
                df = df[['AreaID', f'{service_name}']]

                # Set 'AreaID' as index
                df.set_index('AreaID', inplace=True)

                # Add the dataframe to the list
                df_list.append(df)

# Concatenate all the dataframes in the list into a master dataframe
master_df = pd.concat(df_list, axis=1)

# Reset the index so 'AreaID' becomes a regular column again
master_df.reset_index(inplace=True)

# Sum values for each service (columns with same service name)
master_df = master_df.groupby(master_df.columns, axis=1).sum()

# Avoid DataFrame fragmentation
master_df = master_df.copy()

# Reset index to get 'AreaID' as a column
master_df.reset_index(inplace=True)

# Re-order the columns to ensure 'AreaID' is the first column
ordered_columns = ['AreaID'] + [col for col in master_df.columns if col != 'AreaID']
master_df = master_df[ordered_columns]

# Export the DataFrame to a new CSV file
master_df.to_csv(csv_output_file_path, index=False)

print(f"CSV file generated at: {csv_output_file_path}")

