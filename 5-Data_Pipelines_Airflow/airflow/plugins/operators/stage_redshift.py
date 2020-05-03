from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Transfer data from AWS S3 to staging tables in AWS Redshift
    
    Parameters:
    redshift_conn_id: Connection Id of the Airflow connection to AWS Redshift database (Postgres Type)
    table_name: name of the staging table in AWS Redshift
    aws_credentials_id: Connection Id of the Airflow connection to AWS
    s3_bucket: name of S3 bucket, e.g. "udacity-dend"
    s3_key: name of S3 key. This field is templatable when context is enabled, e.g. "log_data/{execution_date.year}/{execution_date.month}/"
    delimiter: csv field delimiter
    ignore_headers: '0' or '1'
    data_files_format: 'csv' or 'json'
    jsonpaths: path to JSONpaths file
    
    Returns: None
    """
    
    
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    copy_staging_sql = """
        COPY {table_name}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        REGION 'us-west-2'
        {fileformat};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 data_files_format="",
                 jsonpaths="",
                 *args, **kwargs): # time_format ="",

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers = ignore_headers
        self.data_format = data_files_format.lower()
        self.jsonpaths = jsonpaths
        self.delimiter = delimiter
        #self.time_format = time_format
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Cleaning data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table_name))
        
        self.log.info("Staging data from AWS S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        if self.data_format == 'csv':
            file_formatting = "DELIMITER '{}'".format(self.delimiter)
        elif self.data_format == 'json':
            json_option = self.jsonpaths or 'auto'
            file_formatting = "FORMAT AS JSON '{}'".format(json_option)
         
        #if self.time_format != '':
        #    time_str = "timeformat '{}'".format(self.time_format)
        #    file_formatting += time_str
        
        formatted_sql = StageToRedshiftOperator.copy_staging_sql.format(
            table_name = self.table_name,
            s3_path = s3_path,
            access_key = credentials.access_key,
            secret_key = credentials.secret_key,
            fileformat = file_formatting)
        redshift.run(formatted_sql)





