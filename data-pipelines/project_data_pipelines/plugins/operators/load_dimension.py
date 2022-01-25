from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    """
    Loads a dimension table into Redshift from data in the staging tables
    
    Params:
    -  postgres_conn_id: Redshift connection id,
    - sql: query for getting the data and inserting it into the table,
    - table: target table,
    - truncate: wether table content should be deleted or not,
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 task_id='',
                 postgres_conn_id='',
                 sql='',
                 table='',
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.task_id=task_id,
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.table = table
        self.truncate = truncate
        

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id)
        
        if self.truncate:
            self.log.info(f'Truncate table {self.table}')
            postgres.run(f'TRUNCATE {self.table}')

        self.log.info(f'Load dimension table: {self.table}')
        postgres.run(f'INSERT INTO {self.table} {self.sql}')
