import numpy as np
import os
import pandas as pd
import shutil
from time import sleep
from zipfile import ZipFile


def extract_and_unzip_file(zip_file_path, unzipped_folder):
    """
    Extracts all files in zip_file_path to unzipped_folder
    """
    try:
        z = ZipFile(zip_file_path)
        z.extractall(unzipped_folder)
        print(f"Extracted file {zip_file_path} to {unzipped_folder}")
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
    if os.path.exists(os.path.join(original_folder_path, new_folder_name)) == False:
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
    df = pd.read_csv(original_filepath, encoding="cp1252")

    # Remove rows where all values are missing
    df = df.dropna(how="all")

    # Replacing empty strings for float columns with nans
    num_cols = [col for col in df.columns if df[col].dtype in ["float64", "int64"]]
    for col in num_cols:
        df[col] = df[col].fillna(np.nan)
        df.loc[df[col] == "", col] = np.nan

    # Replacing nulls with empty strings for text columns
    cols = [col for col in df.columns if df[col].dtype == "O"]
    for col in cols:
        df[col] = df[col].fillna("")

    # Write into CSV file
    df.to_csv(
        os.path.join(
            cleaned_folder_path,
            os.path.split(original_filepath)[-1].replace(".txt", ".csv"),
        ),
        index=False,
    )


zipped_folder = os.path.join(
    "C:\\Users\\Munira\\Desktop\\demo-pipeline", "zipped_raw_data"
)
unzipped_folder = os.path.join("C:\\Users\\Munira\\Desktop\\demo-pipeline", "raw_data")


if __name__ == "__main__":
    zipped_folder = os.path.join(
        "C:\\Users\\Munira\\Desktop\\demo-pipeline", "Ingestion", "zipped_raw_data"
    )
    unzipped_folder = os.path.join(
        "C:\\Users\\Munira\\Desktop\\demo-pipeline", "Ingestion", "raw_data"
    )
    zip_files = [x for x in os.listdir(zipped_folder) if x.endswith(".zip")]

    for zip_file in zip_files:
        # Extract files
        extract_and_unzip_file(
            zip_file_path=os.path.join(zipped_folder, zip_file),
            unzipped_folder=unzipped_folder,
        )
        # Move processed zip files to subfolder
        move_files_to_new_subfolder(
            new_folder_name="Processed",
            file_identifier=zip_file,
            original_folder_path=zipped_folder,
        )

    # Organize into data vs ReadMe files
    move_files_to_new_subfolder(
        new_folder_name="ReadMeFiles",
        file_identifier="readme",
        original_folder_path=unzipped_folder,
    )
    move_files_to_new_subfolder(
        new_folder_name="DataFiles",
        file_identifier="txt",
        original_folder_path=unzipped_folder,
    )

    # Clean data to prepare for ingestion into postgres and write cleaned data to CSV
    txt_files = [
        x
        for x in os.listdir(os.path.join(unzipped_folder, "DataFiles"))
        if x.endswith(".txt")
    ]
    os.mkdir(os.path.join(unzipped_folder, "CleanedData"))

    for txt_file in txt_files:
        # Clean data and write into CSV files
        clean_raw_data_files(
            os.path.join(unzipped_folder, "DataFiles", txt_file),
            os.path.join(unzipped_folder, "CleanedData"),
        )
        # Move processed raw txt files into Processed subfolder
        move_files_to_new_subfolder(
            new_folder_name="Processed",
            file_identifier=txt_file,
            original_folder_path=os.path.join(unzipped_folder, "DataFiles"),
        )
