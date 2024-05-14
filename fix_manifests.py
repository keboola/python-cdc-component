import json
import os

# Define the directory to search
directory = 'db_components/ex_mysql_cdc/tests/functional/'

# Iterate over all subdirectories and files in the directory
for root, dirs, files in os.walk(directory):
    # Iterate over all files in the current directory
    for file in files:
        # Check if the file is 'inventory_sales.csv.manifest'
        if file.endswith('.manifest'):
            # Construct the full file path
            file_path = os.path.join(root, file)

            # Load the JSON data
            with open(file_path, 'r') as f:
                data = json.load(f)

            data['write_always'] = False

            # Save the modified JSON data
            with open(file_path, 'w') as f:
                json.dump(data, f, separators=(', ', ': '))
