from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
     Quality assurance to check if the tables are not empty
    Params:
    - redshift_connection_id: Redshift Connection id
    - test_tables: Array of tables to check for data quality
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_connection_id="",
                 test_tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = redshift_connection_id
        self.test_tables = test_tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        for table in self.test_tables:
            records = redshift_hook.get_records(
                f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(
                    f"Data quality check failed. {table} contained 0 rows")
            self.log.info(
                f"Data quality on table {table} check passed with {records[0][0]} records")