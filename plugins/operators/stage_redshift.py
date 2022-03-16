from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
 

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """ 
    template_fields = ("s3_key",)
    @apply_defaults
    def __init__(self,
                 redshift_conn='',
                 aws_credentials='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_format='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn=redshift_conn
        self.aws_credentials=aws_credentials
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.json_format=json_format

        

    def execute(self, context):
        
        self.log.info('Getting credentials for AWS')
        awsHook = AwsHook(self.aws_credentials)
        credentials = awsHook.get_credentials()
        
        self.log.info('Establishing connection to redshift')
        redshift = PostgresHook(self.redshift_conn)
        
        self.log.info("Clearing existing data from destination table in Redshift")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f'Copying {self.s3_key} to {self.table} in Redshift')   
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formated_sql=StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format)
        redshift.run(formated_sql)
        
        self.log.info(f'Staging of {self.table} table finished')



