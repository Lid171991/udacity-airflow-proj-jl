from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    # This function is telling the operator what it needs to load the dimension table
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name

                 # A connection to a DB
                 redshift_conn_id,
                 # A table to load the data into
                 origin_table,
                 # The SQL commands to help load the data
                 sql,
                 # How to insert the data into the table
                 insert_mode,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        
        # The next 4 lines save the above info for later
        self.redshift_conn_id=redshift_conn_id
        self.origin_table=origin_table
        self.sql=sql
        self.insert_mode=insert_mode

    # Next, we tell the operator what to do
    def execute(self, context):
        # Log a message stating the operator has yet to be implemented
        self.log.info('LoadDimensionOperator not implemented yet')
        # The next line then creates a hook to connect to the redshift DB
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # The next 3 lines check whether a table needs to be cleared out
        # If it does, a message will be displayed to state it is doing this
        # then once empty, it will load fresh data into the table
        # If not required, it will just insert the data
        if self.insert_mode == 'with truncate':
            self.log.info(f"Truncate and insert data: {self.origin_table}")
            redshift_hook.run(f"Truncate table: {self.origin_table}")
        else:
            self.log.info(f"Insert data: {self.origin_table}")
        # Finally, the SQL statement is run that will load the data in
        redshift_hook.run(self.sql)


