from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name

                 # A DB to connect to
                 redshift_conn_id,
                 # A connection to AWS to stage in Redshift
                 aws_conn_id,
                 # A table to load the data into
                 origin_table,
                 # The location of the data
                 s3_bucket,
                 s3_key,
                 # The format of the data
                 json_path='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

        # Save for use later
        self.redshift_conn_id=redshift_conn_id
        self.aws_conn_id=aws_conn_id
        self.origin_table=origin_table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.json_path=json_path


    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_conn_id), client_type='s3')
        credentials=aws_hook.get_credentials()

        self.log.info("Creating S3 Path")
        s3_path=f"s3://{self.s3_bucket}/{self.s3_key}"

        self.log.info("Forming COPY SQL statement")
        copy_sql = f"""
            COPY {self.origin_table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            FORMAT AS JSON '{self.json_path}';"""

        self.log.info("Connecting to Redshift Database")
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Executing COPY command")
        redshift_hook.run(copy_sql)
        self.log.info("Loaded Data Successfully")











