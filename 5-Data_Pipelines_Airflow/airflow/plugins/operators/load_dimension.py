from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    """
    Creates Dimension Tables in AWS Redshift using the Data from Staging Tables
    
    Parameters:
    redshift_conn_id: Connection Id of the Airflow connection to AWS Redshift database (Postgres Type)
    destination_dim_table: Name of the destination Dimension Table
    sql_statement: SQL statement to INSERT values into the destination table (in Helpers forlder)
    update_mode: 'overwrite' to overwrite existing data or 'insert' to insert new data in the dimension table
    Returns: None
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 destination_dim_table = "",
                 sql_statement = "",
                 update_mode = "overwrite",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_dim_table=destination_dim_table
        self.sql_statement=sql_statement
        self.update_mode = update_mode

    def execute(self, context):
        self.log.info('Fetching redshift hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Loading Dimension {self.destination_dim_table} table')
        if self.update_mode == 'overwrite':
            self.log.info(f'Truncating Dimension {self.destination_dim_table} table')
            update_query = 'TRUNCATE {}; INSERT INTO {} ({})'.format(self.destination_dim_table, self.destination_dim_table, self.sql_statement)
        elif self.update_mode == 'insert':
            self.log.info(f'Inserting Dimension {self.destination_table} table')
            update_query = 'INSERT INTO {} ({})'.format(self.destination_dim_table, self.sql_statement)
        redshift.run(update_query)
