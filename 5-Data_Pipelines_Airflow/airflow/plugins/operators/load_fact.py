from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    """
    Creates Fact Tables in AWS Redshift using the Data from Staging Tables
    
    Parameters:
    redshift_conn_id: Connection Id of the Airflow connection to AWS Redshift database (Postgres Type)
    destination_fact_table: Name of the destination Fact Table
    sql_statement: SQL statement to INSERT values into the destination table (in Helpers forlder)
    
    Returns: None
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 destination_fact_table = "",
                 sql_statement = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_fact_table=destination_fact_table
        self.sql_statement=sql_statement

    def execute(self, context):
        self.log.info('Fetching redshift hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Loading fact table {self.destination_fact_table}')
        insert_query = 'INSERT INTO {} ({})'.format(self.destination_fact_table, self.sql_statement)
        redshift.run(insert_query)
