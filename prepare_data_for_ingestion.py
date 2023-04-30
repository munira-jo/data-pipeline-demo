import os
import pandas as pd
import shutil
from zipfile import ZipFile


def extract_and_unzip_files(zipped_folder, unzipped_folder):
    for zip_filename in os.listdir(zipped_folder):
        z = ZipFile(os.path.join(zipped_folder, zip_filename))
        z.extractall(unzipped_folder)


def move_files_to_new_subfolder(
    new_folder_name, file_identifier, original_file_location
):
    try:
        # Create subfolder
        os.mkdir(os.path.join(unzipped_folder, new_folder_name))
        # Get all files with file_identifier as part of their filenames
        files_list = [
            x
            for x in os.listdir(unzipped_folder)
            if file_identifier.lower() in x.lower()
        ]
        # Move files to new subfolder
        for file in files_list:
            shutil.move(
                os.path.join(original_file_location, file),
                os.path.join(original_file_location, new_folder_name, file),
            )
    except FileExistsError:
        pass
    print("Done")


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

# Extract files
extract_and_unzip_files(zipped_folder, unzipped_folder)

# Organize into data vs ReadMe files
move_files_to_new_subfolder("ReadMeFiles", "Readme", unzipped_folder)
move_files_to_new_subfolder("DataFiles", "txt", unzipped_folder)
