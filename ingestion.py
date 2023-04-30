import os
import psycopg2
import shutil
from zipfile import ZipFile


zipped_folder = os.path.join(
    "C:\\Users\\Munira\\Desktop\\demo-pipeline", "zipped_raw_data"
)
unzipped_folder = os.path.join("C:\\Users\\Munira\\Desktop\\demo-pipeline", "raw_data")


def connect_to_db(db_name, password):
    conn = psycopg2.connect(
        f"dbname={db_name} user=dataengineer host='mydb.cb7sygkkbh0u.us-east-2.rds.amazonaws.com' port=5432 password={password}"
    )
    cur = conn.cursor()
    return conn, cur


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
        files_list = [x for x in os.listdir(unzipped_folder) if file_identifier.lower() in x.lower()]
        # Move files to new subfolder
        for file in files_list:
            shutil.move(
                os.path.join(original_file_location, file),
                os.path.join(original_file_location, new_folder_name, file),
            )
    except FileExistsError:
        pass
    print("Done")


extract_and_unzip_files(zipped_folder, unzipped_folder)

move_files_to_new_subfolder("ReadMeFiles", "Readme", unzipped_folder)

move_files_to_new_subfolder("DataFiles", "txt", unzipped_folder)
