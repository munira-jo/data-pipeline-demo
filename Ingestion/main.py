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
