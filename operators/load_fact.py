from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name

                 # Connection to a DB
                 redshift_conn_id,
                 # SQL query to load fact table into DB
                 sql,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

        # Storing for use later
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    # Telling the operator what we need to do when executing
    def execute(self, context):
        # Log a start message
        self.log.info('LoadFactOperator not implemented yet')
        # Use a hook to connect to the redshift DB
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Execute the SQL query to load the fact table in
        redshift_hook.run(self.sql)
