"""Pre-ingestion data manipulation

This script allows the user to extract the files from a set of 
zipped files stored in the zipped_folder location. It cleans up 
up the extracted files and writes them to CSV files in
the CleanedData folder. ReadMe files are stored in the
ReadMe folder.

This tool expects .zip files to be stored in the zipped_folder
location.

This script requires that `pandas` be installed within the Python
environment you are running this script in.

This file can also be imported as a module and contains the following
functions:

    * extract_and_unzip_file - unzips a given .zip file
    * move_files_to_new_subfolder - moves chosen files to a subfolder within the current folder
    * clean_raw_data_files - cleans up the extracted files to prepare them for ingestion
    * main - the main function of the script
"""


import os
import shutil
from time import sleep
from zipfile import ZipFile

import numpy as np
import pandas as pd


def extract_and_unzip_file(zip_file_path, extracted_data_folder):
    """
    Extracts all files in zip_file_path to extracted_data_folder
    """
    try:
        with ZipFile(zip_file_path) as my_zip_file:
            my_zip_file.extractall(extracted_data_folder)
            print(f"Extracted file {zip_file_path} to {extracted_data_folder}")
    except FileNotFoundError:
        print(f"{zip_file_path} does not exist.")


def move_files_to_new_subfolder(
    *, original_folder_path, new_folder_name, file_identifier
):
    """
    Creates a new subfolder (if the subfolder doesn't already exist) called new_folder_name
    in the folder original_folder_path.  Then moves all files having a name similar to
    file_identifier that are in original_folder_path to the new subfolder.
    """
    if os.path.exists(os.path.join(original_folder_path, new_folder_name)) is False:
        # Create subfolder
        os.mkdir(os.path.join(original_folder_path, new_folder_name))
    # Get all files with file_identifier as part of their filenames
    files_list = [
        x
        for x in os.listdir(original_folder_path)
        if file_identifier.lower() in x.lower()
        and os.path.isfile(os.path.join(original_folder_path, x))
    ]
    # Move files to new subfolder
    for file in files_list:
        try:
            shutil.move(
                os.path.join(original_folder_path, file),
                os.path.join(original_folder_path, new_folder_name, file),
            )
            print(f"Done moving file {file} to new subfolder {new_folder_name}")
        except FileNotFoundError:
            continue
        except PermissionError:
            sleep(10)
            shutil.move(
                os.path.join(original_folder_path, file),
                os.path.join(original_folder_path, new_folder_name, file),
            )


def clean_raw_data_files(original_filepath, cleaned_folder_path):
    """
    Prepares raw data files for ingestion into Postgres DB
    """
    raw_df = pd.read_csv(original_filepath, encoding="cp1252")

    # Remove rows where all values are missing
    raw_df = raw_df.dropna(how="all")

    # Replacing empty strings for float columns with nans
    num_cols = [
        col for col in raw_df.columns if raw_df[col].dtype in ["float64", "int64"]
    ]
    for col in num_cols:
        raw_df[col] = raw_df[col].fillna(np.nan)
        raw_df.loc[raw_df[col] == "", col] = np.nan

    # Replacing nulls with empty strings for text columns
    cols = [col for col in raw_df.columns if raw_df[col].dtype == "O"]
    for col in cols:
        raw_df[col] = raw_df[col].fillna("")

    # Write into CSV file
    raw_df.to_csv(
        os.path.join(
            cleaned_folder_path,
            os.path.split(original_filepath)[-1].replace(".txt", ".csv"),
        ),
        index=False,
    )


def main(*, folder_with_zipped_data, folder_with_extracted_data):
    """
    Extracts files from .zip files, cleans them up and stores
    them as CSV files to be ingested into Postgres.
    """
    zip_files = [x for x in os.listdir(folder_with_zipped_data) if x.endswith(".zip")]
    for zip_file in zip_files:
        # Extract files
        extract_and_unzip_file(
            zip_file_path=os.path.join(folder_with_zipped_data, zip_file),
            extracted_data_folder=folder_with_extracted_data,
        )
        # Move processed zip files to subfolder
        move_files_to_new_subfolder(
            new_folder_name="Processed",
            file_identifier=zip_file,
            original_folder_path=folder_with_zipped_data,
        )

    # Organize into data vs ReadMe files
    move_files_to_new_subfolder(
        new_folder_name="ReadMeFiles",
        file_identifier="readme",
        original_folder_path=folder_with_extracted_data,
    )
    move_files_to_new_subfolder(
        new_folder_name="DataFiles",
        file_identifier="txt",
        original_folder_path=folder_with_extracted_data,
    )

    # Clean data to prepare for ingestion into postgres and write cleaned data to CSV
    txt_files = [
        x
        for x in os.listdir(os.path.join(folder_with_extracted_data, "DataFiles"))
        if x.endswith(".txt")
    ]
    os.mkdir(os.path.join(folder_with_extracted_data, "CleanedData"))

    for txt_file in txt_files:
        # Clean data and write into CSV files
        clean_raw_data_files(
            os.path.join(folder_with_extracted_data, "DataFiles", txt_file),
            os.path.join(folder_with_extracted_data, "CleanedData"),
        )
        # Move processed raw txt files into Processed subfolder
        move_files_to_new_subfolder(
            new_folder_name="Processed",
            file_identifier=txt_file,
            original_folder_path=os.path.join(folder_with_extracted_data, "DataFiles"),
        )


zipped_folder = os.path.join(
    "C:\\Users\\Munira\\Desktop\\demo-pipeline", "ingestion", "zipped_raw_data"
)
unzipped_folder = os.path.join(
    "C:\\Users\\Munira\\Desktop\\demo-pipeline", "ingestion", "raw_data"
)


if __name__ == "__main__":
    main(
        folder_with_zipped_data=zipped_folder,
        folder_with_extracted_data=unzipped_folder,
    )
