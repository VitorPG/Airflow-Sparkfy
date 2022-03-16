from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    insert_sql=""" INSERT INTO {} {} """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn='',
                 table='',
                 sql='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn=redshift_conn
        self.table=table
        self.sql=sql

    def execute(self, context):
        
        self.log.info('Establishing connection to redshift')
        redshift_hook=PostgresHook(self.redshift_conn)
        
        self.log.info(f'Loading  data to {self.table} table')
        formated_sql=LoadFactOperator.insert_sql.format(self.table,self.sql)
        redshift_hook.run(formated_sql)
        self.log.info(f'{self.table} data loaded to fact table')
