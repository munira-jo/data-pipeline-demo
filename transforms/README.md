## Transforms with dbt

All data transformations post-ingestion are done here, using [dbt](https://docs.getdbt.com/docs/introduction) models.


To run all dbt models:

`cd transforms`
`dbt run`

To run a specific model, run `dbt run --select model_path`. 

To run tests, run `dbt test`

### Raw data

The raw data tables are in the `public` schema.

### Star schema

3 marts have been set up:

1. Inspections and violations
2. Crashes
3. Census

The mart tables are in the `public_marts` schema.


