# Data Pipeline Demo

A data pipeline that will ingest zipped data files in a given folder, load them into a postgres database hosted on AWS RDS and then transform the data into a star schema.

Raw zipped data files are placed in the Ingestion/zipped_raw_data folder. 