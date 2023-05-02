## Transforms with dbt

All data transformations post-ingestion are done here, using [dbt](https://docs.getdbt.com/docs/introduction) models.


To run all dbt models:

`cd transforms`
`dbt run`

To run a specific model, run `dbt run --select model_path`. 

To run tests, run `dbt test`