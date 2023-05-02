"""
A data pipeline that ingests raw data from a zipped folder, extracts the data to an unzipped folder,
creates tables in a Postgres database, and copies CSV files from a cleaned data folder into the tables.

This script imports functions from the following modules:
- os
- copy_files_into_postgres
- create_tables
- prepare_data_for_ingestion

To run the pipeline, call the `main()` function in this script.

Example:
    python3 data_pipeline.py

The `main()` function calls the following functions in sequence:
- `prepare_data_for_ingestion.main()`
- `create_tables.main()`
- `copy_files_into_postgres.main()`

See the docstrings of the individual functions for more information about their purpose and parameters.

Note: Before running this script, make sure that the database connection parameters are correctly set in the 
`create_tables.py` and `copy_files_into_postgres.py` modules.
""" 


import os

import copy_files_into_postgres
import create_tables
import prepare_data_for_ingestion

zipped_folder = os.path.join(
    "C:\\Users\\Munira\\Desktop\\demo-pipeline","ingestion", "zipped_raw_data"
)
unzipped_folder = os.path.join("C:\\Users\\Munira\\Desktop\\demo-pipeline","ingestion", "raw_data")

csv_folder_path=os.path.join("C:\\Users\\Munira\\Desktop\\demo-pipeline",'raw_data','CleanedData')

if __name__ == "__main__":
    prepare_data_for_ingestion.main(folder_with_zipped_data=zipped_folder, \
                                    folder_with_extracted_data=unzipped_folder)
    create_tables.main()
    copy_files_into_postgres.main(csv_folder=csv_folder_path)
