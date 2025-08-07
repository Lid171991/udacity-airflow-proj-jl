from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# This creates a new operator based on BaseOperator
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

   
    @apply_defaults
    # The below function tells the operator what DB to connect to & what table to check from the DB
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name

                 # This is the connection to the Redshift DB
                 redshift_conn_id,
                 # This is the table that needs to be checked
                 origin_table
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        
        # The below 2 lines save the redshift_conn_id & origin_table in the object for use later on
        self.redshift_conn_id = redshift_conn_id
        self.origin_table = origin_table


    # The below code then tells the operator what to do when it is executed
    def execute(self, context):
        # To begin with, no DQ check has been performed, so it logs the below message 
        self.log.info('DataQualityOperator not implemented yet')
        # The next line then creates a hook to connect to the redshift DB
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # The following line then uses a SQL statement to return the count of rows in the table, assigning it to 'records'
        # If the query is to return nothing at all, then a ValueError is returned  
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.origin_table}") 
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.origin_table} returned no results")
        # In the next 3 lines below, if the tables exists, but has 0 rows, it will also throw a ValueError
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.origin_table} contained 0 rows")
        # Lastly, if the task has executed successfully, it will log the message below providing the number of records
        self.log.info(f"Data quality on table {self.origin_table} check passed with {records[0][0]} records")
