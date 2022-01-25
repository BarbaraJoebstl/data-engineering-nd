from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    """
    Copies JSON data from S3 to staging tables in Redshift data warehouse
    
    Params:
    - redshift_conn_id: Redshift connection ID
    - aws_credentials_id: AWS credentials ID
    - table: Target staging table in Redshift to copy data into
    - s3_bucket: S3 bucket where JSON data resides
    - s3_key: Path in S3 bucket where JSON data files reside
    - region: AWS Region where the source data is located
    - truncate: wether to delete the table conten or not
    - data_format: data format
    """

    ui_color = '#358140'

    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                {} REGION '{}'
                """
        
    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                aws_credentials_id='',
                table='',
                s3_bucket='',
                s3_key='',
                region='',
                truncate=False,
                data_format='',
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.truncate = truncate
        self.data_format = data_format

    def execute(self, context):
        redshift = PostgresHook(postgress_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        if self.truncate:
            self.log.info(f'Truncate Redshift table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')
        
        rendered_s3_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_s3_key}'
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table, s3_path, credentials.access_key,
            credentials.secret_key, self.data_format, self.region)
        )

        self.log.info(
            (f'Copying data from {s3_path} to Redshift table {self.table}')
        )
        
        redshift.run(formatted_sql)


